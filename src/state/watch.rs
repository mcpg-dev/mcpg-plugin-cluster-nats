use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use async_nats::jetstream::kv::{Operation, Store as KvStore};
use async_trait::async_trait;
use futures::StreamExt;
use mcpg_cluster_api::{ClusterError, Watch, WatchEvent, WatchEventKind, WatchStream};

/// JetStream KV-backed [`Watch`] primitive.
///
/// Wraps a `KvStore` (the same handle used by [`crate::NatsKv`]) and
/// surfaces JetStream KV's native change-notification stream
/// (`watch_all`) to subscribers. Each subscriber gets a separate
/// JetStream consumer; the bucket replays nothing on subscribe
/// (only future operations are observed).
///
/// Created vs Updated is tracked per-subscriber via a small
/// `HashSet<key>`: the FIRST `Put` event a subscriber sees for any
/// given key surfaces as `Created`; subsequent puts are `Updated`.
/// `Delete` / `Purge` operations remove the key from the seen-set
/// so a re-create after delete surfaces as `Created` again.
///
/// Prefix filtering is applied client-side because NATS subject
/// wildcards (`*` / `>`) operate on dotted tokens, while our key
/// space is opaque-string. A bucket-watch is cheap (one consumer)
/// — the filter just drops unmatched events before they reach the
/// caller's stream.
#[derive(Debug)]
pub struct NatsWatch {
    store: KvStore,
}

impl NatsWatch {
    /// Construct a watcher over an already-bootstrapped JS KV
    /// store. Use the same `KvStore` instance the matching
    /// [`crate::NatsKv`] holds so put/delete events from that KV
    /// reach this watch's subscribers.
    pub fn with_store(store: KvStore) -> Self {
        Self { store }
    }
}

#[async_trait]
impl Watch for NatsWatch {
    async fn watch_prefix(&self, prefix: &str) -> Result<WatchStream, ClusterError> {
        let watcher =
            self.store
                .watch_all()
                .await
                .map_err(|e| ClusterError::BackendUnavailable {
                    reason: format!("nats kv watch_all: {e}"),
                })?;
        let prefix_owned = prefix.to_owned();
        let seen_keys: Arc<Mutex<HashSet<String>>> = Arc::new(Mutex::new(HashSet::new()));
        let stream = watcher.filter_map(move |item| {
            let prefix = prefix_owned.clone();
            let seen_keys = Arc::clone(&seen_keys);
            async move {
                match item {
                    Ok(entry) => {
                        if !entry.key.starts_with(&prefix) {
                            return None;
                        }
                        match entry.operation {
                            Operation::Put => {
                                let kind = {
                                    let mut seen = seen_keys
                                        .lock()
                                        .expect("nats watch seen_keys mutex poisoned");
                                    if seen.insert(entry.key.clone()) {
                                        WatchEventKind::Created
                                    } else {
                                        WatchEventKind::Updated
                                    }
                                };
                                Some(Ok(WatchEvent {
                                    key: entry.key,
                                    kind,
                                    value: Some(entry.value),
                                }))
                            }
                            Operation::Delete | Operation::Purge => {
                                {
                                    let mut seen = seen_keys
                                        .lock()
                                        .expect("nats watch seen_keys mutex poisoned");
                                    seen.remove(&entry.key);
                                }
                                Some(Ok(WatchEvent {
                                    key: entry.key,
                                    kind: WatchEventKind::Deleted,
                                    // JS KV delete tombstones don't
                                    // carry the prior value; leave
                                    // `value` None per the trait
                                    // contract.
                                    value: None,
                                }))
                            }
                        }
                    }
                    Err(e) => Some(Err(ClusterError::BackendUnavailable {
                        reason: format!("nats kv watch stream error: {e}"),
                    })),
                }
            }
        });
        Ok(Box::pin(stream))
    }
}
