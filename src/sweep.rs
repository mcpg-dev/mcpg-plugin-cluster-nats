//! The state-bucket sweep.
//!
//! JetStream removes a record only when a stream limit says so, and the
//! state buckets have no `max_age` (a key without a TTL must live). So an
//! expired value and a delete marker stay until something removes them,
//! and a bucket at its byte cap refuses every write (`discard: new`). One
//! replica at a time — the holder of the [`SWEEP_LEASE_KEY`] lease of its
//! state bucket — sweeps every bucket this plugin opened; every replica
//! reports how full each one is.
//!
//! The same task gates the key index. Every replica lets listings read a
//! bucket's index only while no peer without the index is alive and the
//! index is marked complete; a replica that sees such a peer marks the
//! index incomplete. The lease holder completes it again — a backfill —
//! once no such peer has been seen for a peer expiry.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_nats::jetstream::kv::Store as KvStore;
use mcpg_cluster_api::ActiveLease;
use mcpg_plugin_sdk::HostHandle;
use tokio::runtime::Handle;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;

use crate::lease::{self, AcquireParams, NatsJetStreamLeaseHandle};
use crate::peer::{self, PeerCache};
use crate::state::{BucketUsage, NatsKv, SweepReport};

/// The lease whose holder sweeps, followed by `.` and the state bucket's
/// name: gateways that share one NATS but keep their state apart sweep
/// apart.
const SWEEP_LEASE_KEY: &str = "mcpg-kv-sweep";

/// Wait before the first sweep, so a fleet that starts together settles
/// the lease first.
const FIRST_SWEEP_DELAY: Duration = Duration::from_secs(10);

/// Share of a bucket's byte cap above which every report warns.
const FULL_WARN_RATIO: f64 = 0.8;

/// Seconds between ticks of the task when sweeping is off.
pub(crate) const IDLE_INTERVAL_SEC: u64 = 60;

/// How old a delete or purge marker is before a sweep removes it. An older
/// build's `create` reads a marker and then writes over its revision; the
/// grace keeps the marker for that write.
pub(crate) const MARKER_GRACE: Duration = Duration::from_secs(60);

/// The buckets a capability opened by name, shared with the sweep.
pub(crate) type NamedBuckets = Arc<Mutex<HashMap<String, Arc<NatsKv>>>>;

/// What the sweep task needs.
pub(crate) struct SweepParams {
    pub(crate) leases: KvStore,
    pub(crate) fencing: KvStore,
    pub(crate) node_id: String,
    pub(crate) interval: Duration,
    /// Remove expired records and old markers; when off, the task only
    /// reports fill and keeps the key index.
    pub(crate) sweep: bool,
    pub(crate) state: Arc<NatsKv>,
    pub(crate) named: NamedBuckets,
    pub(crate) host: Option<HostHandle>,
    /// The peers this replica sees, with whether each writes the index.
    pub(crate) peers: PeerCache,
    /// How long a peer stays in `peers` after its last heartbeat.
    pub(crate) peer_expiry: Duration,
}

/// The running sweep task; dropping it stops the task (its lease then
/// expires, and another replica takes over).
pub(crate) struct KvSweeper {
    task: JoinHandle<()>,
}

impl KvSweeper {
    pub(crate) fn start(runtime: &Handle, params: SweepParams) -> Self {
        let task = runtime.spawn(run(runtime.clone(), params));
        Self { task }
    }
}

impl Drop for KvSweeper {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn run(runtime: Handle, p: SweepParams) {
    tokio::time::sleep(FIRST_SWEEP_DELAY.min(p.interval)).await;
    let mut tick = tokio::time::interval(p.interval);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut lease: Option<NatsJetStreamLeaseHandle> = None;
    let mut full: HashMap<String, bool> = HashMap::new();
    // When this replica last saw a peer that does not write the index.
    let mut unindexed_seen: Option<std::time::Instant> = None;
    loop {
        tick.tick().await;
        let buckets: Vec<Arc<NatsKv>> = {
            let named = p.named.lock().await;
            std::iter::once(Arc::clone(&p.state))
                .chain(named.values().cloned())
                .collect()
        };
        for kv in &buckets {
            match kv.usage().await {
                Ok(usage) => report_usage(kv.bucket(), usage, p.host.as_ref(), &mut full),
                Err(error) => {
                    tracing::debug!(bucket = kv.bucket(), error = %error, "nats cluster: bucket usage unavailable");
                }
            }
        }

        let unindexed = peer::unindexed_peer_present(&p.peers).await;
        if unindexed {
            unindexed_seen = Some(std::time::Instant::now());
        }
        for kv in &buckets {
            gate_index(kv, unindexed).await;
        }

        if let Some(held) = &lease
            && let Err(error) = held.renew().await
        {
            tracing::info!(error = %error, "nats cluster: lost the KV sweep lease; another replica sweeps");
            lease = None;
        }
        if lease.is_none() {
            let params = AcquireParams {
                leases: p.leases.clone(),
                fencing: p.fencing.clone(),
                key: format!("{SWEEP_LEASE_KEY}.{}", p.state.bucket()),
                node_id: p.node_id.clone(),
                ttl_ms: u64::try_from(p.interval.as_millis().saturating_mul(2)).unwrap_or(u64::MAX),
                wait: false,
                renew_before_expiry_percent: 50,
            };
            match lease::try_acquire_async(runtime.clone(), params).await {
                Ok(Some(held)) => lease = Some(held),
                Ok(None) => continue,
                Err(error) => {
                    tracing::debug!(error = %error, "nats cluster: KV sweep lease unavailable; next tick");
                    continue;
                }
            }
        }

        for kv in &buckets {
            let started = std::time::Instant::now();
            let swept = if p.sweep {
                kv.sweep(MARKER_GRACE).await
            } else {
                Ok(SweepReport::default())
            };
            match swept {
                Ok(report) => {
                    let removed = report.expired + report.markers;
                    if removed > 0 || report.upgraded > 0 {
                        tracing::info!(
                            bucket = kv.bucket(),
                            scanned = report.scanned,
                            expired = report.expired,
                            markers = report.markers,
                            upgraded = report.upgraded,
                            took_ms = started.elapsed().as_millis() as u64,
                            "nats cluster: swept a KV bucket"
                        );
                    }
                    if let Some(host) = p.host.as_ref().filter(|_| p.sweep) {
                        let bucket = kv.bucket();
                        host.counter(
                            "mcpg_cluster_kv_swept_total",
                            report.expired,
                            &[("bucket", bucket), ("reason", "expired")],
                        );
                        host.counter(
                            "mcpg_cluster_kv_swept_total",
                            report.markers,
                            &[("bucket", bucket), ("reason", "marker")],
                        );
                    }
                }
                Err(error) => tracing::warn!(
                    bucket = kv.bucket(),
                    error = %error,
                    "nats cluster: KV sweep failed; expired records stay until the next sweep (the NATS user needs the stream purge API of the bucket)"
                ),
            }

            let settled = unindexed_seen.is_none_or(|seen| seen.elapsed() >= p.peer_expiry);
            if !unindexed && settled && !kv.index_ready() {
                complete_index(kv, &p.peers).await;
            }
        }
    }
}

/// Let listings of `kv` read its index, or not, as this replica sees the
/// fleet now.
async fn gate_index(kv: &NatsKv, unindexed_peer: bool) {
    if unindexed_peer {
        if let Err(error) = kv.invalidate_index().await {
            tracing::debug!(bucket = kv.bucket(), error = %error, "nats cluster: index state unavailable; next tick");
        }
        return;
    }
    match kv.index_completed_at().await {
        Ok(completed) => {
            let ready = completed.is_some();
            if ready != kv.index_ready() {
                tracing::info!(
                    bucket = kv.bucket(),
                    ready,
                    "nats cluster: key listings {} the index",
                    if ready { "read" } else { "no longer read" }
                );
            }
            kv.set_index_ready(ready);
        }
        Err(error) => {
            tracing::debug!(bucket = kv.bucket(), error = %error, "nats cluster: index state unavailable; next tick");
        }
    }
}

/// Index every live key of `kv`, and mark the index complete when no peer
/// without the index appeared meanwhile.
async fn complete_index(kv: &NatsKv, peers: &PeerCache) {
    let started_at_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0);
    let started = std::time::Instant::now();
    match kv.backfill_index().await {
        Ok(indexed) => {
            if peer::unindexed_peer_present(peers).await {
                return;
            }
            if let Err(error) = kv.mark_index_complete(started_at_ms).await {
                tracing::warn!(bucket = kv.bucket(), error = %error, "nats cluster: the key index could not be marked complete; next sweep");
                return;
            }
            kv.set_index_ready(true);
            tracing::info!(
                bucket = kv.bucket(),
                indexed,
                took_ms = started.elapsed().as_millis() as u64,
                "nats cluster: the key index is complete; key listings read it"
            );
        }
        Err(error) => tracing::warn!(
            bucket = kv.bucket(),
            error = %error,
            "nats cluster: the key index backfill failed; key listings scan every record until the next sweep"
        ),
    }
}

/// Publish a bucket's fill as gauges, and warn while it is above
/// [`FULL_WARN_RATIO`] of its cap.
fn report_usage(
    bucket: &str,
    usage: BucketUsage,
    host: Option<&HostHandle>,
    full: &mut HashMap<String, bool>,
) {
    if let Some(host) = host {
        let labels = [("bucket", bucket)];
        host.gauge("mcpg_cluster_kv_bucket_bytes", usage.bytes as f64, &labels);
        host.gauge(
            "mcpg_cluster_kv_bucket_messages",
            usage.messages as f64,
            &labels,
        );
        host.gauge(
            "mcpg_cluster_kv_bucket_max_bytes",
            usage.max_bytes as f64,
            &labels,
        );
    }
    let ratio = (usage.max_bytes > 0).then(|| usage.bytes as f64 / usage.max_bytes as f64);
    let over = ratio.is_some_and(|r| r >= FULL_WARN_RATIO);
    let was = full.insert(bucket.to_owned(), over).unwrap_or(false);
    if over {
        tracing::warn!(
            bucket,
            bytes = usage.bytes,
            max_bytes = usage.max_bytes,
            messages = usage.messages,
            "nats cluster: a KV bucket is above 80% of its byte cap; at the cap every write fails — raise jetstream.state_bucket_max_bytes (or named_bucket_max_bytes)"
        );
    } else if was {
        tracing::info!(
            bucket,
            bytes = usage.bytes,
            max_bytes = usage.max_bytes,
            "nats cluster: a KV bucket is below 80% of its byte cap again"
        );
    }
}
