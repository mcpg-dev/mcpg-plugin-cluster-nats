use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_nats::jetstream::Context as JsContext;
use async_nats::jetstream::consumer::DeliverPolicy;
use async_nats::jetstream::consumer::push::{Ordered, OrderedConfig};
use async_nats::jetstream::context::PublishErrorKind;
use async_nats::jetstream::kv::{Operation, Store as KvStore};
use async_nats::{HeaderMap, HeaderValue};
use async_trait::async_trait;
use bytes::Bytes;
use futures::{StreamExt, TryStreamExt};
use mcpg_cluster_api::{ClusterError, Entry, KeyValueStore, KvPage};

use super::index;

/// JetStream KV-backed state.
///
/// One `NatsKv` instance == one KV bucket. Multiple capabilities
/// pointing at the same bucket share the underlying NATS connection
/// only if operators explicitly arrange it (the type itself opens
/// a fresh connection per `connect`).
///
/// Every write publishes the record itself, with an [`EXPIRES_HEADER`],
/// so [`NatsKv::sweep`] can find expired records without reading values.
///
/// Every live key also has an index record (see [`index`]), written
/// create-only in flight behind the data record: beside an index record
/// that is there already, the server stores nothing. An index subject
/// holds one live record or nothing, never a marker, so a write whose
/// index record is refused as a conflict is indexed. A delete or an
/// expiry leaves the index record; a listing reads the data record of
/// every index hit and skips a dead key. The sweep removes the index
/// record with the dead data record: the index record, the data record,
/// then a new read of the key, which writes the index record back when a
/// concurrent write brought the key back.
#[derive(Debug)]
pub struct NatsKv {
    store: KvStore,
    /// The connection the bucket was opened on; listings take their
    /// consumer inboxes from it.
    client: async_nats::Client,
    /// The JetStream context writes publish on.
    js: JsContext,
    /// What a write's subject starts with, before the encoded key: the
    /// bucket's subject prefix, behind the JetStream API prefix when the
    /// context has a domain.
    write_prefix: String,
    /// Listings read the index: every writer of the fleet writes it and a
    /// backfill completed it. Until then they scan every record.
    index_ready: AtomicBool,
}

impl NatsKv {
    /// Construct a `NatsKv` over an already-bootstrapped JS KV store, the
    /// connection and JetStream context it was opened on, and that
    /// context's API prefix (`$JS.API`, or `$JS.<domain>.API`). Used by
    /// `mcpg-plugin-cluster-nats` to share its single connection across
    /// the coordinator + the primitive accessors instead of opening a
    /// fresh connection per primitive.
    pub fn new(
        store: KvStore,
        client: async_nats::Client,
        js: JsContext,
        api_prefix: &str,
    ) -> Self {
        let subject_prefix = store
            .put_prefix
            .clone()
            .unwrap_or_else(|| store.prefix.clone());
        let write_prefix = if store.use_jetstream_prefix {
            format!("{api_prefix}.{subject_prefix}")
        } else {
            subject_prefix
        };
        Self {
            store,
            client,
            js,
            write_prefix,
            index_ready: AtomicBool::new(false),
        }
    }

    /// The bucket's name.
    pub fn bucket(&self) -> &str {
        &self.store.name
    }

    /// Let listings read the index, or make them scan every record.
    pub fn set_index_ready(&self, ready: bool) {
        self.index_ready.store(ready, Ordering::Release);
    }

    /// Whether listings read the index.
    pub fn index_ready(&self) -> bool {
        self.index_ready.load(Ordering::Acquire)
    }
}

/// Header of every record this store writes: the record's absolute expiry
/// in Unix seconds, `0` for none. A record without it comes from an older
/// build, and a sweep reads its value to learn the expiry.
const EXPIRES_HEADER: &str = "Mcpg-Expires";

/// The JetStream header that makes a publish a CAS on the subject's last
/// sequence (`0` = the subject holds nothing).
const EXPECTED_LAST_SUBJECT_SEQUENCE: &str = "Nats-Expected-Last-Subject-Sequence";

/// What a key holds, as a write sees it.
enum Slot {
    /// No record.
    Empty,
    /// A delete marker or an expired value: absent, but a CAS over it must
    /// name its revision.
    Dead(u64),
    /// A live value.
    Live {
        value: Bytes,
        expires_at_secs: Option<u64>,
        revision: u64,
    },
}

/// What a write expects the key to hold.
#[derive(Debug, Clone, Copy)]
enum Expect {
    Anything,
    Nothing,
    Revision(u64),
}

/// A failed write.
enum WriteError {
    /// The key did not hold what the write expected.
    Conflict,
    Failed(ClusterError),
}

/// A record on its way to the server: [`Self::done`] waits for its ack.
/// Records published on one connection are stored in the order they were
/// published, so several can be in flight while their acks are awaited.
struct PendingWrite(async_nats::jetstream::context::PublishAckFuture);

impl PendingWrite {
    /// The record's revision once the server stored it.
    async fn done(self) -> Result<u64, WriteError> {
        match self.0.await {
            Ok(ack) => Ok(ack.sequence),
            Err(e) if e.kind() == PublishErrorKind::WrongLastSequence => Err(WriteError::Conflict),
            Err(e) => Err(WriteError::Failed(ClusterError::BackendUnavailable {
                reason: format!("nats kv write: {e}"),
            })),
        }
    }
}

/// How many index records a backfill has in flight at once.
const BACKFILL_IN_FLIGHT: usize = 256;

/// The most index subjects a listing reads from the stream's subject list;
/// a prefix with more is listed through a consumer.
const SUBJECT_LISTING_MAX: usize = 10_000;

/// How fast the server delivers the scan of a sweep or a backfill, in bits
/// a second (about 18 000 headers-only records a second): the scan of a
/// large bucket spreads over minutes instead of holding up the writes of
/// the fleet. The scans read every record (`DeliverPolicy::All`): with
/// history 1 each is the latest of its key, and the server builds no
/// per-subject view first.
const MAINTENANCE_SCAN_BPS: u64 = 32 * 1024 * 1024;

/// The consumer of a listing, a sweep or a backfill, deleted from the
/// server when dropped: an ephemeral consumer outlives its reader by its
/// inactive threshold (30 s), and every record the bucket stores meanwhile
/// signals it.
struct RecordConsumer {
    records: Ordered,
    stream: async_nats::jetstream::stream::Stream,
    name: String,
}

impl Drop for RecordConsumer {
    fn drop(&mut self) {
        delete_consumer_later(&self.stream, std::mem::take(&mut self.name));
    }
}

/// Delete the consumer `name` of `stream` in the background.
fn delete_consumer_later(stream: &async_nats::jetstream::stream::Stream, name: String) {
    let Ok(runtime) = tokio::runtime::Handle::try_current() else {
        return;
    };
    let stream = stream.clone();
    runtime.spawn(async move {
        let _ = stream.delete_consumer(&name).await;
    });
}

/// What one [`NatsKv::sweep`] did.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct SweepReport {
    /// Records looked at.
    pub scanned: u64,
    /// Expired values removed.
    pub expired: u64,
    /// Delete and purge markers removed.
    pub markers: u64,
    /// Records of older builds that now carry the expiry header.
    pub upgraded: u64,
}

/// What a bucket holds, and its byte cap (`-1` = none).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BucketUsage {
    pub bytes: u64,
    pub messages: u64,
    pub max_bytes: i64,
}

impl NatsKv {
    async fn slot(&self, encoded: &str, key: &str) -> Result<Slot, ClusterError> {
        let entry =
            self.store
                .entry(encoded)
                .await
                .map_err(|e| ClusterError::BackendUnavailable {
                    reason: format!("nats kv entry `{key}`: {e}"),
                })?;
        Ok(match entry {
            None => Slot::Empty,
            Some(e) if e.operation != Operation::Put => Slot::Dead(e.revision),
            Some(e) => {
                let (value, expires_at_secs) = decode_value(e.value);
                if is_expired(expires_at_secs) {
                    Slot::Dead(e.revision)
                } else {
                    Slot::Live {
                        value,
                        expires_at_secs,
                        revision: e.revision,
                    }
                }
            }
        })
    }

    /// Publish `value` as the record of `encoded`, with its expiry in the
    /// value envelope (what older builds read) and in [`EXPIRES_HEADER`].
    /// Returns the record's revision.
    async fn write(
        &self,
        encoded: &str,
        value: &[u8],
        expires_at_secs: Option<u64>,
        expect: Expect,
    ) -> Result<u64, WriteError> {
        self.publish(encoded, value, expires_at_secs, expect)
            .await
            .map_err(WriteError::Failed)?
            .done()
            .await
    }

    /// [`Self::write`] without waiting for the ack.
    async fn publish(
        &self,
        encoded: &str,
        value: &[u8],
        expires_at_secs: Option<u64>,
        expect: Expect,
    ) -> Result<PendingWrite, ClusterError> {
        let mut headers = HeaderMap::new();
        headers.insert(
            EXPIRES_HEADER,
            HeaderValue::from(expires_at_secs.unwrap_or(0)),
        );
        match expect {
            Expect::Anything => {}
            Expect::Nothing => {
                headers.insert(EXPECTED_LAST_SUBJECT_SEQUENCE, HeaderValue::from(0u64))
            }
            Expect::Revision(revision) => {
                headers.insert(EXPECTED_LAST_SUBJECT_SEQUENCE, HeaderValue::from(revision))
            }
        }
        self.publish_record(encoded, headers, encode_value(value, expires_at_secs))
            .await
    }

    /// Publish a delete marker for `encoded` without waiting for the ack.
    async fn publish_marker(&self, encoded: &str) -> Result<PendingWrite, ClusterError> {
        let mut headers = HeaderMap::new();
        headers.insert("KV-Operation", HeaderValue::from("DEL"));
        self.publish_record(encoded, headers, Bytes::new()).await
    }

    async fn publish_record(
        &self,
        encoded: &str,
        headers: HeaderMap,
        payload: Bytes,
    ) -> Result<PendingWrite, ClusterError> {
        self.js
            .publish_with_headers(format!("{}{encoded}", self.write_prefix), headers, payload)
            .await
            .map(PendingWrite)
            .map_err(|e| ClusterError::BackendUnavailable {
                reason: format!("nats kv write: {e}"),
            })
    }

    /// Publish the index record of `key`, create-only, without waiting for
    /// the ack.
    async fn publish_index(&self, key: &str) -> Result<PendingWrite, ClusterError> {
        self.publish(&index::index_key(key), b"", None, Expect::Nothing)
            .await
    }

    /// Wait for the ack of an index record. Returns its revision when this
    /// write stored it, `None` when the key had an index record already.
    async fn index_done(pending: PendingWrite) -> Result<Option<u64>, ClusterError> {
        match pending.done().await {
            Ok(revision) => Ok(Some(revision)),
            Err(WriteError::Conflict) => Ok(None),
            Err(WriteError::Failed(e)) => Err(e),
        }
    }

    /// Remove the records of the stream subject `subject` up to and
    /// including `revision`; a later record survives.
    async fn purge_through(&self, subject: &str, revision: u64) -> Result<(), ClusterError> {
        self.store
            .stream
            .purge()
            .filter(subject)
            .sequence(revision + 1)
            .await
            .map(|_| ())
            .map_err(|e| ClusterError::BackendUnavailable {
                reason: format!("nats kv purge: {e}"),
            })
    }

    /// Write the index record of `key`.
    async fn write_index(&self, key: &str) -> Result<(), ClusterError> {
        Self::index_done(self.publish_index(key).await?)
            .await
            .map(|_| ())
    }

    /// Remove the index record of `key`.
    async fn purge_index(&self, key: &str) -> Result<(), ClusterError> {
        self.store
            .stream
            .purge()
            .filter(format!("{}{}", self.store.prefix, index::index_key(key)))
            .await
            .map(|_| ())
            .map_err(|e| ClusterError::BackendUnavailable {
                reason: format!("nats kv index purge `{key}`: {e}"),
            })
    }

    /// Write the index record of `key` when its data record is live.
    async fn reindex_if_live(&self, key: &str) -> Result<(), ClusterError> {
        if matches!(self.slot(&encode_key(key), key).await?, Slot::Live { .. }) {
            self.write_index(key).await?;
        }
        Ok(())
    }

    /// Remove the index record of `key` whose data record the bucket
    /// refused, then index the key again when it is live: a concurrent
    /// write whose index record was refused beside the removed one is
    /// indexed again.
    async fn unindex(&self, key: &str) -> Result<(), ClusterError> {
        self.purge_index(key).await?;
        self.reindex_if_live(key).await
    }

    /// Write the index record of every live key. Returns the number of keys
    /// indexed. The caller marks the index complete when no writer without
    /// the index was seen meanwhile.
    pub async fn backfill_index(&self) -> Result<u64, ClusterError> {
        let mut indexed = 0;
        // Up to BACKFILL_IN_FLIGHT index records in flight at once: a bucket
        // of a million keys is indexed in about a minute, not a round trip
        // per key. A key indexed already stores nothing.
        let mut in_flight: Vec<PendingWrite> = Vec::with_capacity(BACKFILL_IN_FLIGHT);
        if let Some(mut consumer) = self
            .record_consumer(
                "mcpg kv backfill",
                ">",
                DeliverPolicy::All,
                MAINTENANCE_SCAN_BPS,
            )
            .await?
        {
            let now = now_unix_secs();
            loop {
                let record =
                    match tokio::time::timeout(LIST_IDLE_TIMEOUT, consumer.records.next()).await {
                        Ok(Some(Ok(record))) => record,
                        Ok(Some(Err(e))) => {
                            return Err(ClusterError::BackendUnavailable {
                                reason: format!("nats kv backfill record: {e}"),
                            });
                        }
                        Ok(None) => break,
                        Err(_) => {
                            return Err(ClusterError::BackendUnavailable {
                                reason: "nats kv backfill: no record arrived".to_owned(),
                            });
                        }
                    };
                let pending = record
                    .info()
                    .map_err(|e| ClusterError::BackendUnavailable {
                        reason: format!("nats kv backfill record metadata: {e}"),
                    })?
                    .pending;
                let expired = record
                    .headers
                    .as_ref()
                    .and_then(|h| h.get(EXPIRES_HEADER))
                    .and_then(|v| v.as_str().parse::<u64>().ok())
                    .is_some_and(|at| at != 0 && at <= now);
                if record_operation(record.headers.as_ref()) == Operation::Put
                    && !expired
                    && let Some(encoded) = record.subject.strip_prefix(self.store.prefix.as_str())
                    && !index::is_index_key(encoded)
                    && let Some(key) = decode_key(encoded)
                {
                    in_flight.push(self.publish_index(&key).await?);
                    indexed += 1;
                    if in_flight.len() == BACKFILL_IN_FLIGHT {
                        for pending in in_flight.drain(..) {
                            Self::index_done(pending).await?;
                        }
                    }
                }
                if pending == 0 {
                    break;
                }
            }
        }
        for pending in in_flight {
            Self::index_done(pending).await?;
        }
        Ok(indexed)
    }

    /// Record that the index holds every live key, from a backfill that
    /// started at `started_at_ms` (Unix ms).
    pub async fn mark_index_complete(&self, started_at_ms: u64) -> Result<(), ClusterError> {
        self.write(
            index::INDEX_STATE_KEY,
            started_at_ms.to_string().as_bytes(),
            None,
            Expect::Anything,
        )
        .await
        .map(|_| ())
        .map_err(|e| match e {
            WriteError::Failed(e) => e,
            WriteError::Conflict => ClusterError::Internal {
                reason: "nats kv index state: an unconditional write reported a conflict".into(),
            },
        })
    }

    /// When the index was last made complete (Unix ms); `None` when it is
    /// not complete.
    pub async fn index_completed_at(&self) -> Result<Option<u64>, ClusterError> {
        Ok(
            match self.slot(index::INDEX_STATE_KEY, "index state").await? {
                Slot::Live { value, .. } => std::str::from_utf8(&value)
                    .ok()
                    .and_then(|s| s.parse::<u64>().ok()),
                Slot::Empty | Slot::Dead(_) => None,
            },
        )
    }

    /// Mark the index incomplete: a writer without it is alive.
    pub async fn invalidate_index(&self) -> Result<(), ClusterError> {
        self.set_index_ready(false);
        if self.index_completed_at().await?.is_some() {
            self.store
                .delete(index::INDEX_STATE_KEY)
                .await
                .map_err(|e| ClusterError::BackendUnavailable {
                    reason: format!("nats kv index invalidate: {e}"),
                })?;
        }
        Ok(())
    }

    /// An ordered, headers-only consumer over the keys of the bucket that
    /// match `filter` (a subject filter after the bucket's prefix), that
    /// the server delivers at most `rate_limit` bits a second (`0`: as fast
    /// as the reader reads).
    async fn record_consumer(
        &self,
        description: &str,
        filter: &str,
        deliver_policy: DeliverPolicy,
        rate_limit: u64,
    ) -> Result<Option<RecordConsumer>, ClusterError> {
        let consumer = self
            .store
            .stream
            .create_consumer(OrderedConfig {
                deliver_subject: self.client.new_inbox(),
                description: Some(description.to_owned()),
                filter_subject: format!("{}{filter}", self.store.prefix),
                headers_only: true,
                deliver_policy,
                rate_limit,
                ..Default::default()
            })
            .await
            .map_err(|e| ClusterError::BackendUnavailable {
                reason: format!("nats kv {description} consumer: {e}"),
            })?;
        let name = consumer.cached_info().name.clone();
        if consumer.cached_info().num_pending == 0 {
            delete_consumer_later(&self.store.stream, name);
            return Ok(None);
        }
        match consumer.messages().await {
            Ok(records) => Ok(Some(RecordConsumer {
                records,
                stream: self.store.stream.clone(),
                name,
            })),
            Err(e) => {
                delete_consumer_later(&self.store.stream, name);
                Err(ClusterError::BackendUnavailable {
                    reason: format!("nats kv {description} messages: {e}"),
                })
            }
        }
    }

    /// The data keys under `prefix` that have an index record, in byte
    /// order, from the stream's subject list (no consumer); `None` when
    /// there are more than `max`.
    async fn indexed_keys(
        &self,
        prefix: &str,
        max: usize,
    ) -> Result<Option<Vec<String>>, ClusterError> {
        let filter = format!("{}{}", self.store.prefix, index::prefix_filter(prefix));
        let unavailable =
            |e: async_nats::jetstream::stream::InfoError| ClusterError::BackendUnavailable {
                reason: format!("nats kv list subjects: {e}"),
            };
        let mut subjects = self
            .store
            .stream
            .info_with_subjects(&filter)
            .await
            .map_err(unavailable)?;
        let mut keys = Vec::new();
        while let Some((subject, _)) = subjects.try_next().await.map_err(unavailable)? {
            if let Some(key) = subject
                .strip_prefix(self.store.prefix.as_str())
                .and_then(index::data_key)
                && key.starts_with(prefix)
            {
                if keys.len() == max {
                    return Ok(None);
                }
                keys.push(key);
            }
        }
        keys.sort_unstable();
        Ok(Some(keys))
    }

    /// The live entries of `keys`, read in parallel, in the order given; a
    /// dead key's index record stays until the sweep removes it with the
    /// data record.
    async fn live_entries(&self, keys: Vec<String>) -> Result<Vec<(String, Entry)>, ClusterError> {
        Ok(futures::stream::iter(keys)
            .map(|key| async move {
                let entry = match self.slot(&encode_key(&key), &key).await? {
                    Slot::Live {
                        value,
                        expires_at_secs,
                        ..
                    } => Some((
                        key,
                        Entry {
                            bytes: value,
                            expires_at: expires_at_secs
                                .map(|s| UNIX_EPOCH + Duration::from_secs(s)),
                        },
                    )),
                    Slot::Empty | Slot::Dead(_) => None,
                };
                Ok::<_, ClusterError>(entry)
            })
            .buffered(LIST_FETCH_PARALLELISM)
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .flatten()
            .collect())
    }

    /// One page of a listing over the sorted `keys`: the keys after
    /// `after`; the cursor is the last key of the page.
    async fn subjects_page(
        &self,
        keys: &[String],
        after: Option<&str>,
        limit: usize,
    ) -> Result<KvPage, ClusterError> {
        let start = after.map_or(0, |after| keys.partition_point(|k| k.as_str() <= after));
        let page: Vec<String> = keys[start..].iter().take(limit).cloned().collect();
        let next = (start + page.len() < keys.len())
            .then(|| page.last().map(|k| format!("s{k}")))
            .flatten();
        Ok(KvPage {
            entries: self.live_entries(page).await?,
            next,
        })
    }

    /// Remove what the bucket keeps after it is gone: expired values and
    /// delete markers older than `marker_grace`. Each removal stops at the
    /// sequence the sweep read, so a write made during the sweep survives.
    /// A record an older build wrote carries no expiry header: its value is
    /// read once, and it is removed when expired or else rewritten with the
    /// header (a CAS, so a concurrent write wins).
    pub async fn sweep(&self, marker_grace: Duration) -> Result<SweepReport, ClusterError> {
        let mut report = SweepReport::default();
        let Some(mut consumer) = self
            .record_consumer(
                "mcpg kv sweep",
                ">",
                DeliverPolicy::All,
                MAINTENANCE_SCAN_BPS,
            )
            .await?
        else {
            return Ok(report);
        };
        let now = now_unix_secs();
        let mut legacy: Vec<(String, u64)> = Vec::new();
        loop {
            let record =
                match tokio::time::timeout(LIST_IDLE_TIMEOUT, consumer.records.next()).await {
                    Ok(Some(Ok(record))) => record,
                    Ok(Some(Err(e))) => {
                        return Err(ClusterError::BackendUnavailable {
                            reason: format!("nats kv sweep record: {e}"),
                        });
                    }
                    // The rest waits for the next sweep.
                    Ok(None) | Err(_) => break,
                };
            let info = record
                .info()
                .map_err(|e| ClusterError::BackendUnavailable {
                    reason: format!("nats kv sweep record metadata: {e}"),
                })?;
            let (sequence, pending) = (info.stream_sequence, info.pending);
            let published = u64::try_from(info.published.unix_timestamp()).unwrap_or(0);
            report.scanned += 1;
            let subject = record.subject.as_str();
            match record_operation(record.headers.as_ref()) {
                Operation::Put => {
                    let expiry = record
                        .headers
                        .as_ref()
                        .and_then(|h| h.get(EXPIRES_HEADER))
                        .and_then(|v| v.as_str().parse::<u64>().ok());
                    match expiry {
                        Some(at) if at != 0 && at <= now => {
                            self.remove_dead(subject, sequence).await?;
                            report.expired += 1;
                        }
                        Some(_) => {}
                        None => {
                            if let Some(encoded) = subject.strip_prefix(self.store.prefix.as_str())
                            {
                                legacy.push((encoded.to_owned(), sequence));
                            }
                        }
                    }
                }
                Operation::Delete | Operation::Purge => {
                    if now.saturating_sub(published) >= marker_grace.as_secs() {
                        self.remove_dead(subject, sequence).await?;
                        report.markers += 1;
                    }
                }
            }
            if pending == 0 {
                break;
            }
        }

        drop(consumer);
        let outcomes: Vec<Result<LegacyOutcome, ClusterError>> = futures::stream::iter(legacy)
            .map(|(encoded, sequence)| async move { self.settle_legacy(&encoded, sequence).await })
            .buffer_unordered(LIST_FETCH_PARALLELISM)
            .collect()
            .await;
        for outcome in outcomes {
            match outcome? {
                LegacyOutcome::Expired => report.expired += 1,
                LegacyOutcome::Upgraded => report.upgraded += 1,
                LegacyOutcome::Moved => {}
            }
        }
        Ok(report)
    }

    /// Remove the dead record at `sequence` of `subject` (an expired value
    /// or a marker) with the index record of its key. A later record of the
    /// subject survives.
    async fn remove_dead(&self, subject: &str, sequence: u64) -> Result<(), ClusterError> {
        let encoded = subject
            .strip_prefix(self.store.prefix.as_str())
            .unwrap_or(subject);
        if index::is_index_key(encoded) {
            // No write here leaves a marker on an index subject: after
            // removing one, index the key again when it is live.
            self.purge_through(subject, sequence).await?;
            if let Some(key) = index::data_key(encoded) {
                self.reindex_if_live(&key).await?;
            }
            return Ok(());
        }
        let Some(key) = decode_key(encoded) else {
            return self.purge_through(subject, sequence).await;
        };
        // The new read comes after both removals. A write that read the key
        // live (an `incr`, an `expire`) writes no index record, and its CAS
        // fails once the dead record is gone: every write that won is in the
        // bucket before the read.
        self.purge_index(&key).await?;
        self.purge_through(subject, sequence).await?;
        self.reindex_if_live(&key).await
    }

    /// Remove an older build's record at `sequence` when it is expired, or
    /// rewrite it with the expiry header.
    async fn settle_legacy(
        &self,
        encoded: &str,
        sequence: u64,
    ) -> Result<LegacyOutcome, ClusterError> {
        let entry =
            self.store
                .entry(encoded)
                .await
                .map_err(|e| ClusterError::BackendUnavailable {
                    reason: format!("nats kv sweep entry: {e}"),
                })?;
        let Some(entry) = entry.filter(|e| e.revision == sequence && e.operation == Operation::Put)
        else {
            return Ok(LegacyOutcome::Moved);
        };
        let (value, expires_at_secs) = decode_value(entry.value);
        let key = decode_key(encoded);
        if is_expired(expires_at_secs) {
            self.remove_dead(&format!("{}{encoded}", self.store.prefix), sequence)
                .await?;
            return Ok(LegacyOutcome::Expired);
        }
        match self
            .write(encoded, &value, expires_at_secs, Expect::Revision(sequence))
            .await
        {
            Ok(_) => {
                // An older build wrote no index record.
                if let Some(key) = &key {
                    self.write_index(key).await?;
                }
                Ok(LegacyOutcome::Upgraded)
            }
            Err(WriteError::Conflict) => Ok(LegacyOutcome::Moved),
            Err(WriteError::Failed(e)) => Err(e),
        }
    }

    /// What the bucket holds now, and its byte cap.
    pub async fn usage(&self) -> Result<BucketUsage, ClusterError> {
        let mut stream = self.store.stream.clone();
        let info = stream
            .info()
            .await
            .map_err(|e| ClusterError::BackendUnavailable {
                reason: format!("nats kv usage: {e}"),
            })?;
        Ok(BucketUsage {
            bytes: info.state.bytes,
            messages: info.state.messages,
            max_bytes: info.config.max_bytes,
        })
    }
}

/// What a sweep did with an older build's record.
enum LegacyOutcome {
    Expired,
    Upgraded,
    /// Rewritten or removed since the sweep read it.
    Moved,
}

/// How long a listing waits for the next record before it gives up on the
/// page.
const LIST_IDLE_TIMEOUT: Duration = Duration::from_secs(5);

/// Values a listing reads at once.
const LIST_FETCH_PARALLELISM: usize = 32;

/// Attempts of a revision-CAS write (`expire`) before it reports a conflict.
const CAS_WRITE_ATTEMPTS: u32 = 8;

/// The operation a record of the bucket's stream carries: a value, or a
/// delete / purge marker.
fn record_operation(headers: Option<&async_nats::HeaderMap>) -> Operation {
    let Some(headers) = headers else {
        return Operation::Put;
    };
    match headers.get("KV-Operation").map(|v| v.as_str()) {
        Some("DEL") => Operation::Delete,
        Some("PURGE") => Operation::Purge,
        Some(_) => Operation::Put,
        None => match headers.get("Nats-Marker-Reason").map(|v| v.as_str()) {
            Some("Remove") => Operation::Delete,
            Some(_) => Operation::Purge,
            None => Operation::Put,
        },
    }
}

/// JetStream KV keys are restricted to `[-/_=.A-Za-z0-9]` and must not
/// start or end with `.`. The gateway's cluster keys (`pipeline:{id}`,
/// `pending_req:{id}`, `session:{id}`, the boot-probe key, …) carry `:`
/// and other separators that JS KV rejects, so every key crossing this
/// primitive is escaped into the JS-KV-safe alphabet.
///
/// The escape is a per-byte homomorphism: each byte outside the safe
/// passthrough set (`A-Za-z0-9` + `-` + `_` + `/`) becomes `=HH` (the
/// uppercase hex of the byte), and `=` itself escapes to `=3D`. Because
/// each input byte maps to a fixed output independent of position, the
/// encoding preserves prefixes — so `list_prefix` can encode the query
/// prefix and prefix-match in encoded space — and is exactly reversible
/// for the keys it returns to the caller. `.` and `=` are deliberately
/// NOT in the passthrough set: dropping `.` sidesteps the no-leading/
/// trailing-`.` rule, and reserving `=` keeps the escape unambiguous.
fn encode_key(key: &str) -> String {
    let mut out = String::with_capacity(key.len());
    for &b in key.as_bytes() {
        if b.is_ascii_alphanumeric() || b == b'-' || b == b'_' || b == b'/' {
            out.push(b as char);
        } else {
            out.push('=');
            out.push_str(&format!("{b:02X}"));
        }
    }
    out
}

/// Reverse [`encode_key`]. Returns the original key, or `None` if the
/// stored key is not well-formed escaped output (which should not occur
/// for keys this primitive wrote).
fn decode_key(encoded: &str) -> Option<String> {
    let bytes = encoded.as_bytes();
    let mut out: Vec<u8> = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'=' {
            if i + 2 >= bytes.len() {
                return None;
            }
            let hi = (bytes[i + 1] as char).to_digit(16)?;
            let lo = (bytes[i + 2] as char).to_digit(16)?;
            out.push((hi * 16 + lo) as u8);
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8(out).ok()
}

/// Inline TTL envelope marker for NATS JetStream KV values.
///
/// JetStream KV has no native per-key TTL — only a bucket-wide
/// `max_age`. To honor the [`KeyValueStore`] per-key TTL contract (the
/// one redis satisfies natively), a value written with a TTL
/// is stored as `MAGIC ‖ u64-BE(expires_at_secs) ‖ value`, and the
/// expiry is decoded and enforced on read. Values without a TTL are
/// stored verbatim, so the common path stays byte-identical and is
/// recovered by `decode_value`'s no-envelope fallback. The marker leads
/// with a NUL and embeds an ASCII tag, so real gateway KV payloads
/// (JSON / length-prefixed session+pipeline state) never collide.
const TTL_ENVELOPE_MAGIC: &[u8] = b"\x00mcpg-kv-ttl\x01";

fn now_unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Absolute expiry (Unix seconds) `ttl` from now, clamped to ≥1s so a
/// sub-second TTL never rounds down to an already-expired record.
fn expires_at_from(ttl: Option<Duration>) -> Option<u64> {
    ttl.map(|d| now_unix_secs() + d.as_secs().max(1))
}

/// Wrap `value` with an optional inline expiry. See [`TTL_ENVELOPE_MAGIC`].
fn encode_value(value: &[u8], expires_at_secs: Option<u64>) -> Bytes {
    match expires_at_secs {
        Some(secs) => {
            let mut buf = Vec::with_capacity(TTL_ENVELOPE_MAGIC.len() + 8 + value.len());
            buf.extend_from_slice(TTL_ENVELOPE_MAGIC);
            buf.extend_from_slice(&secs.to_be_bytes());
            buf.extend_from_slice(value);
            Bytes::from(buf)
        }
        None => Bytes::copy_from_slice(value),
    }
}

/// Inverse of [`encode_value`]: the original bytes plus the decoded
/// expiry (`None` when the value carries no envelope).
fn decode_value(stored: Bytes) -> (Bytes, Option<u64>) {
    let header = TTL_ENVELOPE_MAGIC.len() + 8;
    if stored.len() >= header && stored.starts_with(TTL_ENVELOPE_MAGIC) {
        let off = TTL_ENVELOPE_MAGIC.len();
        let secs = u64::from_be_bytes(
            stored[off..off + 8]
                .try_into()
                .expect("8 bytes guaranteed by the length check"),
        );
        (stored.slice(header..), Some(secs))
    } else {
        (stored, None)
    }
}

/// True when an inline expiry has elapsed (logically absent).
fn is_expired(expires_at_secs: Option<u64>) -> bool {
    expires_at_secs.is_some_and(|s| s <= now_unix_secs())
}

/// Pause between lost CAS rounds: linear growth plus sub-millisecond
/// clock-derived decorrelation, so contenders that collided in one
/// round don't re-collide in lock-step on the next.
async fn cas_backoff(attempt: u32) {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| u64::from(d.subsec_nanos()))
        .unwrap_or(0);
    tokio::time::sleep(Duration::from_micros(
        u64::from(attempt) * 500 + nanos % 700,
    ))
    .await;
}

#[async_trait]
impl KeyValueStore for NatsKv {
    async fn get(&self, key: &str) -> Result<Option<Entry>, ClusterError> {
        // A logically-expired record reads as absent; the sweep removes it.
        match self.slot(&encode_key(key), key).await? {
            Slot::Live {
                value,
                expires_at_secs,
                ..
            } => Ok(Some(Entry {
                bytes: value,
                expires_at: expires_at_secs.map(|s| UNIX_EPOCH + Duration::from_secs(s)),
            })),
            Slot::Empty | Slot::Dead(_) => Ok(None),
        }
    }

    async fn put(
        &self,
        key: &str,
        value: Bytes,
        ttl: Option<Duration>,
    ) -> Result<(), ClusterError> {
        // The index record in flight behind the data record (see `NatsKv`):
        // a put costs one round trip, and a put over a live key stores one
        // record.
        let data = self
            .publish(
                &encode_key(key),
                &value,
                expires_at_from(ttl),
                Expect::Anything,
            )
            .await?;
        let index = self.publish_index(key).await?;
        let written = data.done().await;
        let indexed = Self::index_done(index).await;
        match written {
            Ok(_) => indexed.map(|_| ()),
            Err(WriteError::Failed(e)) => {
                // A full bucket refuses the value and may take the small
                // index record behind it; that record must not stay.
                if matches!(indexed, Ok(Some(_))) {
                    let _ = self.unindex(key).await;
                }
                Err(e)
            }
            Err(WriteError::Conflict) => Err(ClusterError::CasConflict {
                key: key.to_owned(),
                reason: "nats kv put: an unconditional write reported a conflict".to_owned(),
            }),
        }
    }

    async fn put_if_absent(
        &self,
        key: &str,
        value: Bytes,
        ttl: Option<Duration>,
    ) -> Result<bool, ClusterError> {
        // A write that expects an empty subject is the atomic
        // put-if-absent, but it sees PHYSICAL presence, and an expired
        // value or a delete marker stays in the bucket until a sweep
        // removes it. Such a record counts as absent: take it over with a
        // CAS on its revision, so of N racers exactly one wins. A lost CAS
        // is a loss only when a live record took the key; a sweep that
        // removed the dead record meanwhile just means the next CAS
        // expects nothing.
        let encoded = encode_key(key);
        let expires_at_secs = expires_at_from(ttl);
        let mut expect = Expect::Nothing;
        for attempt in 0..CAS_WRITE_ATTEMPTS {
            if attempt > 1 {
                cas_backoff(attempt).await;
            }
            // The index record in flight behind the claim, as in `put`:
            // beside a live key it stores nothing, so a lost claim writes
            // nothing.
            let claim = self
                .publish(&encoded, &value, expires_at_secs, expect)
                .await?;
            let index = self.publish_index(key).await?;
            let claimed = claim.done().await;
            let indexed = Self::index_done(index).await;
            match claimed {
                Ok(_) => return indexed.map(|_| true),
                Err(WriteError::Conflict) => {}
                Err(WriteError::Failed(e)) => {
                    // As in `put`: a refused claim leaves no index record.
                    if matches!(indexed, Ok(Some(_))) {
                        let _ = self.unindex(key).await;
                    }
                    return Err(e);
                }
            }
            expect = match self.slot(&encoded, key).await? {
                Slot::Live { .. } => return Ok(false),
                Slot::Dead(revision) => Expect::Revision(revision),
                Slot::Empty => Expect::Nothing,
            };
        }
        Err(ClusterError::CasConflict {
            key: key.to_owned(),
            reason: format!("nats kv put_if_absent: revision CAS lost {CAS_WRITE_ATTEMPTS} rounds"),
        })
    }

    async fn delete(&self, key: &str) -> Result<bool, ClusterError> {
        // Delete markers need only the publish permission a write has; the
        // sweep removes them once they are a minute old, and the key's index
        // record with them (see `NatsKv`).
        let encoded = encode_key(key);
        let existed = matches!(self.slot(&encoded, key).await?, Slot::Live { .. });
        match self.publish_marker(&encoded).await?.done().await {
            Ok(_) | Err(WriteError::Conflict) => Ok(existed),
            Err(WriteError::Failed(e)) => Err(e),
        }
    }

    async fn list_page(
        &self,
        prefix: &str,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<KvPage, ClusterError> {
        // The bucket keeps one record per key (history 1), so the records
        // from a stream sequence on are the latest state of every key whose
        // last write is that recent. A page reads them in sequence order,
        // headers only, and its cursor is the last sequence it read: a key
        // that is not rewritten during the pass sits at one sequence and
        // is read exactly once.
        //
        // When the index is ready, the page reads index records under the
        // prefix's filter instead — the server skips every other key. A
        // prefix of up to SUBJECT_LISTING_MAX index records is listed from
        // the stream's subject list in key order, which needs no consumer;
        // a larger one through a consumer of its index records. A pass
        // keeps the way its first page read (the cursor says which), so a
        // pass does not switch halfway.
        let limit = limit.max(1);
        if let Some(after) = cursor.and_then(|c| c.strip_prefix('s')) {
            let keys = self
                .indexed_keys(prefix, usize::MAX)
                .await?
                .unwrap_or_default();
            return self.subjects_page(&keys, Some(after), limit).await;
        }
        if cursor.is_none()
            && self.index_ready()
            && let Some(keys) = self.indexed_keys(prefix, SUBJECT_LISTING_MAX).await?
        {
            return self.subjects_page(&keys, None, limit).await;
        }
        let (indexed, after) = match cursor {
            None => (self.index_ready(), 0),
            Some(c) => {
                let (indexed, sequence) = match c.as_bytes().first() {
                    Some(b'i') => (true, &c[1..]),
                    Some(b'd') => (false, &c[1..]),
                    _ => (false, c),
                };
                let after = sequence
                    .parse::<u64>()
                    .map_err(|_| ClusterError::Precondition {
                        reason: format!("nats kv list: `{c}` is not a cursor of this store"),
                    })?;
                (indexed, after)
            }
        };
        let filter = if indexed {
            index::prefix_filter(prefix)
        } else {
            ">".to_owned()
        };
        let Some(mut consumer) = self
            .record_consumer(
                "mcpg kv list",
                &filter,
                DeliverPolicy::ByStartSequence {
                    start_sequence: after + 1,
                },
                0,
            )
            .await?
        else {
            return Ok(KvPage::default());
        };

        // Data keys come back ENCODED. The escape is prefix-preserving, so
        // match the encoded prefix and decode each matched key. An index
        // key decodes to its data key, checked against the prefix whole.
        let encoded_prefix = encode_key(prefix);
        let mut matched: Vec<String> = Vec::new();
        let mut last_sequence: Option<u64> = None;
        let mut more = true;
        while matched.len() < limit {
            let record =
                match tokio::time::timeout(LIST_IDLE_TIMEOUT, consumer.records.next()).await {
                    Ok(Some(Ok(record))) => record,
                    Ok(Some(Err(e))) => {
                        return Err(ClusterError::BackendUnavailable {
                            reason: format!("nats kv list record: {e}"),
                        });
                    }
                    Ok(None) => break,
                    // A stall with progress hands the rest to the next page; a
                    // stall without progress is not an end of the pass.
                    Err(_) if last_sequence.is_some() => break,
                    Err(_) => {
                        return Err(ClusterError::BackendUnavailable {
                            reason: "nats kv list: no record arrived".to_owned(),
                        });
                    }
                };
            let info = record
                .info()
                .map_err(|e| ClusterError::BackendUnavailable {
                    reason: format!("nats kv list record metadata: {e}"),
                })?;
            last_sequence = Some(info.stream_sequence);
            more = info.pending > 0;
            if let Some(encoded) = record.subject.strip_prefix(self.store.prefix.as_str()) {
                if indexed {
                    // The data record decides, as in a subject list: a
                    // marker on an index subject does not hide a live key.
                    if let Some(key) = index::data_key(encoded)
                        && key.starts_with(prefix)
                    {
                        matched.push(key);
                    }
                } else if record_operation(record.headers.as_ref()) == Operation::Put
                    && encoded.starts_with(&encoded_prefix)
                    && let Some(key) = decode_key(encoded)
                {
                    matched.push(key);
                }
            }
            if !more {
                break;
            }
        }

        drop(consumer);
        let entries = self.live_entries(matched).await?;
        let mode = if indexed { 'i' } else { 'd' };
        Ok(KvPage {
            entries,
            next: if more {
                last_sequence.map(|s| format!("{mode}{s}"))
            } else {
                None
            },
        })
    }

    async fn expire(&self, key: &str, ttl: Option<Duration>) -> Result<bool, ClusterError> {
        // JetStream KV has no native per-key TTL, so "set the TTL" means
        // re-writing the value with a fresh inline-expiry envelope, by a CAS
        // on the revision read so a concurrent write is not undone. The
        // value is preserved; only the expiry changes (`ttl: None` clears
        // it). An absent or expired key reports false.
        let encoded = encode_key(key);
        for attempt in 0..CAS_WRITE_ATTEMPTS {
            if attempt > 0 {
                cas_backoff(attempt).await;
            }
            let Slot::Live {
                value, revision, ..
            } = self.slot(&encoded, key).await?
            else {
                return Ok(false);
            };
            match self
                .write(
                    &encoded,
                    &value,
                    expires_at_from(ttl),
                    Expect::Revision(revision),
                )
                .await
            {
                Ok(_) => return Ok(true),
                Err(WriteError::Conflict) => {}
                Err(WriteError::Failed(e)) => return Err(e),
            }
        }
        Err(ClusterError::CasConflict {
            key: key.to_owned(),
            reason: format!("nats kv expire: revision CAS lost {CAS_WRITE_ATTEMPTS} rounds"),
        })
    }

    async fn incr(
        &self,
        key: &str,
        delta: i64,
        ttl: Option<Duration>,
    ) -> Result<i64, ClusterError> {
        // Atomicity rides the bucket's per-key revision: read the record,
        // compute the new count, then write it expecting what was read
        // (nothing, a dead record's revision, or the live one's) — an
        // atomic single-winner op. A lost race backs off (each round only
        // one contender can win, so N-way storms need up to N rounds) and
        // retries. The counter rides the same inline-TTL envelope as every
        // other value in this bucket, so `get` and listings round-trip it.
        const CAS_ATTEMPTS: u32 = 64;
        let encoded = encode_key(key);
        for attempt in 0..CAS_ATTEMPTS {
            if attempt > 0 {
                cas_backoff(attempt).await;
            }
            // A counter that comes into being (again) needs its index record.
            let (current, expires_at_secs, expect, creates) = match self.slot(&encoded, key).await?
            {
                Slot::Empty => (0, expires_at_from(ttl), Expect::Nothing, true),
                Slot::Dead(revision) => (0, expires_at_from(ttl), Expect::Revision(revision), true),
                Slot::Live {
                    value,
                    expires_at_secs,
                    revision,
                } => {
                    // ttl=None leaves a live counter's expiry untouched.
                    let expires = if ttl.is_some() {
                        expires_at_from(ttl)
                    } else {
                        expires_at_secs
                    };
                    (
                        mcpg_cluster_api::parse_counter(&value)?,
                        expires,
                        Expect::Revision(revision),
                        false,
                    )
                }
            };
            let next = current
                .checked_add(delta)
                .ok_or_else(mcpg_cluster_api::counter_overflow)?;
            let write = self
                .publish(
                    &encoded,
                    next.to_string().as_bytes(),
                    expires_at_secs,
                    expect,
                )
                .await?;
            // As in `put_if_absent`, the index record in flight behind the
            // write; one that a lost CAS leaves is a stale one (see `NatsKv`).
            let index = if creates {
                Some(self.publish_index(key).await?)
            } else {
                None
            };
            let written = write.done().await;
            let indexed = match index {
                Some(index) => Self::index_done(index).await,
                None => Ok(None),
            };
            match written {
                Ok(_) => return indexed.map(|_| next),
                Err(WriteError::Conflict) => {}
                Err(WriteError::Failed(e)) => {
                    if matches!(indexed, Ok(Some(_))) {
                        let _ = self.unindex(key).await;
                    }
                    return Err(e);
                }
            }
        }
        Err(ClusterError::CasConflict {
            key: key.to_owned(),
            reason: format!("nats kv incr: revision CAS lost {CAS_ATTEMPTS} rounds"),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Mirror of async-nats' `is_valid_key` (it is crate-private): keys
    /// must be non-empty, not start/end with `.`, and contain only
    /// `[-/_=.A-Za-z0-9]`.
    fn is_valid_key(key: &str) -> bool {
        if key.is_empty() || key.starts_with('.') || key.ends_with('.') {
            return false;
        }
        key.bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'/' | b'_' | b'=' | b'.'))
    }

    #[test]
    fn encode_produces_valid_js_kv_keys() {
        // Representative gateway cluster keys all carry `:`, which JS KV
        // rejects raw; the escape must yield a valid key for every one.
        for raw in [
            "mcpg:cluster:boot-probe:0bc445acc516477d8586f035e2b6dccd",
            "pipeline:abc-123",
            "pending_req:srv-7",
            "session:s/1",
            "pipeline-claim:abc:0",
            "request_state/handle.7",
        ] {
            let enc = encode_key(raw);
            assert!(
                is_valid_key(&enc),
                "encoded `{enc}` (from `{raw}`) is not a valid JS KV key"
            );
        }
    }

    #[test]
    fn encode_decode_round_trips() {
        for raw in [
            "mcpg:cluster:boot-probe:deadbeef",
            "pipeline:abc-123",
            "a.b.c",
            "with=equals:and/slash",
            "unicode-✓-key",
            "trailing.",
        ] {
            let enc = encode_key(raw);
            assert_eq!(decode_key(&enc).as_deref(), Some(raw));
        }
    }

    #[test]
    fn value_envelope_round_trips_and_falls_back_to_raw() {
        // No TTL → stored verbatim, decoded as raw with no expiry.
        let raw = Bytes::from_static(b"v1");
        let stored = encode_value(&raw, None);
        assert_eq!(stored.as_ref(), b"v1");
        let (bytes, exp) = decode_value(stored);
        assert_eq!(bytes.as_ref(), b"v1");
        assert_eq!(exp, None);

        // With TTL → enveloped, decoded back to the original bytes + expiry.
        let enveloped = encode_value(&raw, Some(1_900_000_000));
        assert!(enveloped.starts_with(TTL_ENVELOPE_MAGIC));
        let (bytes, exp) = decode_value(enveloped);
        assert_eq!(bytes.as_ref(), b"v1");
        assert_eq!(exp, Some(1_900_000_000));

        // A raw value short enough to not hold a full header decodes as raw.
        let (bytes, exp) = decode_value(Bytes::from_static(b"\x00mcpg"));
        assert_eq!(bytes.as_ref(), b"\x00mcpg");
        assert_eq!(exp, None);
    }

    #[test]
    fn is_expired_honors_the_boundary() {
        assert!(!is_expired(None));
        assert!(is_expired(Some(0)));
        assert!(!is_expired(Some(now_unix_secs() + 3600)));
    }

    #[test]
    fn encode_preserves_prefixes() {
        // The list_prefix path relies on prefix-matching in encoded space.
        let key = encode_key("pipeline:abc:0");
        let prefix = encode_key("pipeline:abc");
        assert!(key.starts_with(&prefix));
        // A non-matching raw prefix must not match in encoded space either.
        let other = encode_key("session:abc");
        assert!(!key.starts_with(&other));
    }
}
