//! Peer presence + heartbeat machinery.
//!
//! Each gateway replica runs:
//!
//! 1. A **heartbeat publisher** task that emits a JSON payload
//!    (`{node_id, address, started_at}`) on the
//!    `mcpg.peers.heartbeat.{node_id}` subject every
//!    `heartbeat_interval_sec`.
//! 2. A **subscriber** task that listens on
//!    `mcpg.peers.heartbeat.>` and folds each received payload
//!    into a `HashMap<node_id, PeerEntry>` under
//!    `Arc<RwLock<...>>`.
//! 3. A **sweeper** that runs alongside the subscriber and
//!    re-classifies / evicts entries past `peer_expiry_sec`.
//!
//! `list_peers` reads a snapshot of the cache. `watch_peers`
//! subscribes to delta events emitted from the folder + sweeper
//! paths.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_nats::Client as NatsClient;
use bytes::Bytes;
use futures::StreamExt;
use mcpg_cluster_api::{ClusterPeer, PeerEvent, PeerHealth};
use serde::{Deserialize, Serialize};
use tokio::runtime::Handle;
use tokio::sync::RwLock;
use tokio::task::AbortHandle;

use crate::config::ClusterNatsConfig;
use crate::pubsub::PeerEventSender;

/// Subject used for heartbeats. Single token after the prefix
/// (`{node_id}`) so subscribers can use a `*` wildcard.
const HEARTBEAT_SUBJECT_PREFIX: &str = "mcpg.peers.heartbeat";

/// Extract the node id from a heartbeat subject's trailing token, or
/// `None` for a malformed subject (wrong prefix, empty token, or a
/// deeper dotted subject). The subject token — gated by NATS publish
/// ACLs — is the authoritative peer identity; the payload's self-asserted
/// `node_id` is not trusted.
fn subject_node_id(subject: &str) -> Option<&str> {
    let token = subject.strip_prefix(&format!("{HEARTBEAT_SUBJECT_PREFIX}."))?;
    if token.is_empty() || token.contains('.') {
        return None;
    }
    Some(token)
}

/// On-wire heartbeat payload. Kept tiny so each pulse fits in a
/// single NATS message.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct HeartbeatPayload {
    node_id: String,
    address: String,
    started_at: String,
}

/// In-memory per-peer record. `last_seen` is a monotonic Instant
/// (more reliable than clock for expiry); the wire-shape `last_seen`
/// surfaced via `ClusterPeer` is rendered just-in-time as RFC3339.
#[derive(Debug, Clone)]
pub(crate) struct PeerEntry {
    pub(crate) node_id: String,
    pub(crate) address: String,
    pub(crate) last_seen: Instant,
    pub(crate) last_seen_rfc3339: String,
    pub(crate) health: PeerHealth,
}

pub(crate) type PeerCache = Arc<RwLock<HashMap<String, PeerEntry>>>;

/// Composite handle held on the plugin struct. Drop aborts both
/// the heartbeat publisher and the subscriber tasks.
pub(crate) struct PeerWorkers {
    pub(crate) cache: PeerCache,
    publisher_abort: AbortHandle,
    subscriber_abort: AbortHandle,
}

impl Drop for PeerWorkers {
    fn drop(&mut self) {
        self.abort();
    }
}

impl PeerWorkers {
    /// Proactively abort the heartbeat publisher + subscriber tasks so
    /// `shutdown()` tears them down within the host's drain window rather
    /// than waiting for `Drop`. `AbortHandle::abort` is idempotent, so
    /// the subsequent `Drop` is a safe no-op.
    pub(crate) fn abort(&self) {
        self.publisher_abort.abort();
        self.subscriber_abort.abort();
    }
}

impl PeerWorkers {
    pub(crate) fn start(
        rt: &Handle,
        nats: NatsClient,
        cfg: &ClusterNatsConfig,
        started_at_rfc3339: String,
        events: PeerEventSender,
    ) -> Self {
        let cache: PeerCache = Arc::new(RwLock::new(HashMap::new()));
        let heartbeat_interval = Duration::from_secs(cfg.node.heartbeat_interval_sec);
        let peer_expiry = Duration::from_secs(cfg.node.peer_expiry_sec);
        let degraded_after = Duration::from_secs(cfg.node.heartbeat_interval_sec * 2);
        let node_id = cfg.node.id.clone();
        let address = cfg.node.address.clone().unwrap_or_default();

        let publisher = rt.spawn(publisher_loop(
            nats.clone(),
            heartbeat_interval,
            HeartbeatPayload {
                node_id: node_id.clone(),
                address: address.clone(),
                started_at: started_at_rfc3339,
            },
        ));

        let subscriber = rt.spawn(subscriber_loop(
            nats,
            Arc::clone(&cache),
            heartbeat_interval,
            degraded_after,
            peer_expiry,
            node_id,
            events,
        ));

        Self {
            cache,
            publisher_abort: publisher.abort_handle(),
            subscriber_abort: subscriber.abort_handle(),
        }
    }

    /// Snapshot the cache as `Vec<ClusterPeer>` for `list_peers`.
    /// Excludes the local node — peer == "another node".
    pub(crate) fn snapshot(&self, rt: &Handle) -> Vec<ClusterPeer> {
        rt.block_on(self.snapshot_async())
    }

    /// Async core for [`snapshot`]. Used directly by the
    /// in-process async `ClusterBackend::list_peers` path.
    pub(crate) async fn snapshot_async(&self) -> Vec<ClusterPeer> {
        let guard = self.cache.read().await;
        guard
            .values()
            .map(|e| ClusterPeer {
                node_id: e.node_id.clone(),
                address: e.address.clone(),
                last_seen: e.last_seen_rfc3339.clone(),
                health: e.health,
                roles: Vec::new(),
            })
            .collect()
    }
}

async fn publisher_loop(nats: NatsClient, interval: Duration, payload: HeartbeatPayload) {
    let subject = format!("{HEARTBEAT_SUBJECT_PREFIX}.{}", payload.node_id);
    let serialised = serde_json::to_vec(&payload).unwrap_or_else(|_| b"{}".to_vec());
    let mut tick = tokio::time::interval(interval);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    // Fire one heartbeat immediately so peers learn about us within
    // a few hundred ms of startup, not a full interval later.
    if let Err(e) = nats
        .publish(subject.clone(), Bytes::from(serialised.clone()))
        .await
    {
        tracing::warn!(
            subject = %subject,
            error = %e,
            "nats cluster: initial heartbeat publish failed"
        );
    }
    loop {
        tick.tick().await;
        if let Err(e) = nats
            .publish(subject.clone(), Bytes::from(serialised.clone()))
            .await
        {
            tracing::warn!(
                subject = %subject,
                error = %e,
                "nats cluster: heartbeat publish failed; retrying next tick"
            );
        }
    }
}

async fn subscriber_loop(
    nats: NatsClient,
    cache: PeerCache,
    sweep_interval: Duration,
    degraded_after: Duration,
    expiry_after: Duration,
    self_node_id: String,
    events: PeerEventSender,
) {
    let pattern = format!("{HEARTBEAT_SUBJECT_PREFIX}.*");
    let mut sub = match nats.subscribe(pattern.clone()).await {
        Ok(s) => s,
        Err(e) => {
            tracing::error!(
                subject = %pattern,
                error = %e,
                "nats cluster: heartbeat subscribe failed; peer cache will stay empty"
            );
            return;
        }
    };

    let mut sweeper = tokio::time::interval(sweep_interval);
    sweeper.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            // Receive heartbeat → fold into cache.
            msg = sub.next() => {
                let Some(msg) = msg else {
                    tracing::warn!("nats cluster: heartbeat subscription stream ended");
                    return;
                };
                // The accepted node id is the subject's trailing token (gated by
                // NATS publish ACLs), NOT the payload's self-asserted node_id.
                let Some(subject_id) = subject_node_id(msg.subject.as_str()) else {
                    tracing::warn!(
                        subject = %msg.subject,
                        "nats cluster: heartbeat on malformed subject; skipping"
                    );
                    continue;
                };
                let node_id = subject_id.to_owned();
                if node_id == self_node_id {
                    // Heartbeat echoed our own node — ignore.
                    continue;
                }
                // Parse the payload only for the advisory `address` (self-reported).
                let address = match serde_json::from_slice::<HeartbeatPayload>(&msg.payload) {
                    Ok(p) => {
                        if p.node_id != node_id {
                            tracing::warn!(
                                subject_id = %node_id,
                                claimed = %p.node_id,
                                "nats cluster: heartbeat node_id mismatch; trusting subject token"
                            );
                        }
                        p.address
                    }
                    Err(e) => {
                        tracing::warn!(
                            subject = %msg.subject,
                            error = %e,
                            "nats cluster: heartbeat decode failed; skipping"
                        );
                        continue;
                    }
                };
                let entry = PeerEntry {
                    node_id: node_id.clone(),
                    address: address.clone(),
                    last_seen: Instant::now(),
                    last_seen_rfc3339: now_rfc3339(),
                    health: PeerHealth::Healthy,
                };
                let mut guard = cache.write().await;
                // Detect Joined vs HealthChanged before inserting so the
                // broadcast carries the right `kind`.
                let prior_health = guard.get(&node_id).map(|e| e.health);
                let snapshot = ClusterPeer {
                    node_id: entry.node_id.clone(),
                    address: entry.address.clone(),
                    last_seen: entry.last_seen_rfc3339.clone(),
                    health: entry.health,
                    roles: Vec::new(),
                };
                guard.insert(node_id.clone(), entry);
                drop(guard);
                match prior_health {
                    None => {
                        // New peer — broadcast Joined.
                        let _ = events.send(PeerEvent::Joined { peer: snapshot });
                    }
                    Some(prev) if prev != PeerHealth::Healthy => {
                        let _ = events.send(PeerEvent::HealthChanged {
                            node_id,
                            health: PeerHealth::Healthy,
                        });
                    }
                    _ => { /* steady-state Healthy heartbeat — no event */ }
                }
            }
            // Sweep tick → reclassify or evict stale entries.
            // We collect the deltas before dropping the write lock
            // so we can broadcast outside the critical section.
            _ = sweeper.tick() => {
                let now = Instant::now();
                let mut deltas: Vec<PeerEvent> = Vec::new();
                {
                    let mut guard = cache.write().await;
                    guard.retain(|node_id, entry| {
                        let age = now.duration_since(entry.last_seen);
                        if age > expiry_after {
                            deltas.push(PeerEvent::Left {
                                node_id: node_id.clone(),
                            });
                            return false;
                        }
                        let new_health = if age > degraded_after {
                            PeerHealth::Degraded
                        } else {
                            PeerHealth::Healthy
                        };
                        if entry.health != new_health {
                            entry.health = new_health;
                            deltas.push(PeerEvent::HealthChanged {
                                node_id: node_id.clone(),
                                health: new_health,
                            });
                        }
                        true
                    });
                }
                for event in deltas {
                    let _ = events.send(event);
                }
            }
        }
    }
}

fn now_rfc3339() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;
    let (y, m, d, h, mn, s) = crate::epoch_to_ymdhms(secs);
    format!("{y:04}-{m:02}-{d:02}T{h:02}:{mn:02}:{s:02}Z")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subject_node_id_extracts_trailing_token() {
        assert_eq!(subject_node_id("mcpg.peers.heartbeat.nodeA"), Some("nodeA"));
        assert_eq!(
            subject_node_id("mcpg.peers.heartbeat.gateway-1_pod"),
            Some("gateway-1_pod")
        );
    }

    #[test]
    fn subject_node_id_rejects_prefix_only_and_multitoken() {
        // Empty trailing token, a deeper dotted subject (which would let a
        // crafted publisher smuggle a `.`-bearing id), and unrelated subjects
        // all yield None.
        assert_eq!(subject_node_id("mcpg.peers.heartbeat."), None);
        assert_eq!(subject_node_id("mcpg.peers.heartbeat.a.b"), None);
        assert_eq!(subject_node_id("mcpg.peers.heartbeat"), None);
        assert_eq!(subject_node_id("some.other.subject"), None);
    }
}
