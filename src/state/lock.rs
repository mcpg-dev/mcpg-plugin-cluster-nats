use async_nats::jetstream::kv::{Operation, Store as KvStore};
use async_trait::async_trait;
use mcpg_cluster_api::{ClusterError, FenceToken, Lease, LeaseHandle};
use serde::{Deserialize, Serialize};
use std::time::{Duration, SystemTime};

/// NATS JetStream KV-backed coordinated leases.
///
/// Acquire: KV `create` (atomic, fails if key exists) writes
/// `holder + fence_token`. Renew: KV `update` with the holder's
/// known revision (CAS — fails if another holder has rotated).
/// Release: KV `delete` after CAS check.
///
/// Fence tokens are derived from the bucket's monotonically-
/// increasing per-key revision number — reliable across crashes.
#[derive(Debug)]
pub struct NatsLock {
    store: KvStore,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LeaseRecord {
    holder: String,
    /// Wall-clock expiry (best-effort; bucket's max_age is the
    /// authoritative expiry for the underlying KV entry).
    expires_at_secs: u64,
}

impl NatsLock {
    /// Construct a `NatsLock` over an already-bootstrapped JS KV
    /// store. Used by `mcpg-plugin-cluster-nats` to share a single
    /// connection across the four primitive accessors. Caller is
    /// responsible for picking a bucket distinct from the
    /// coordinator's own `leases_bucket` to avoid key collisions.
    pub fn with_store(store: KvStore) -> Self {
        Self { store }
    }

    fn now_secs() -> u64 {
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0)
    }
}

#[async_trait]
impl Lease for NatsLock {
    async fn try_acquire(
        &self,
        name: &str,
        holder: &str,
        ttl: Duration,
    ) -> Result<Option<LeaseHandle>, ClusterError> {
        let now = Self::now_secs();
        let expires_at_secs = now + ttl.as_secs().max(1);

        // Check current state first (so same-holder refresh works).
        let current_entry =
            self.store
                .entry(name)
                .await
                .map_err(|e| ClusterError::BackendUnavailable {
                    reason: format!("nats lease entry `{name}`: {e}"),
                })?;

        let payload = serde_json::to_vec(&LeaseRecord {
            holder: holder.to_owned(),
            expires_at_secs,
        })
        .map_err(|e| ClusterError::Internal {
            reason: format!("encode lease record: {e}"),
        })?;

        let revision = match current_entry {
            None => {
                // Atomic create — fails if someone else inserted concurrently.
                match self.store.create(name, payload.into()).await {
                    Ok(rev) => rev,
                    Err(_) => return Ok(None), // Another acquirer won the race.
                }
            }
            Some(e) if e.operation == Operation::Delete || e.operation == Operation::Purge => {
                // Tombstone — atomic create on the new revision.
                match self.store.create(name, payload.into()).await {
                    Ok(rev) => rev,
                    Err(_) => return Ok(None),
                }
            }
            Some(entry) => {
                // Existing entry. Decode + compare holder.
                let rec: LeaseRecord =
                    serde_json::from_slice(&entry.value).map_err(|e| ClusterError::Internal {
                        reason: format!("decode lease record: {e}"),
                    })?;
                if rec.expires_at_secs <= now {
                    // Expired — try to claim via CAS.
                    match self
                        .store
                        .update(name, payload.into(), entry.revision)
                        .await
                    {
                        Ok(rev) => rev,
                        Err(_) => return Ok(None),
                    }
                } else if rec.holder == holder {
                    // Same-holder refresh — CAS update.
                    match self
                        .store
                        .update(name, payload.into(), entry.revision)
                        .await
                    {
                        Ok(rev) => rev,
                        Err(_) => return Ok(None),
                    }
                } else {
                    // Held by another live owner.
                    return Ok(None);
                }
            }
        };

        Ok(Some(LeaseHandle {
            name: name.to_owned(),
            holder: holder.to_owned(),
            // Fence token = JetStream-monotonic revision number.
            fence: FenceToken(revision),
            expires_at: SystemTime::UNIX_EPOCH + Duration::from_secs(expires_at_secs),
        }))
    }

    async fn renew(&self, lease: &LeaseHandle, ttl: Duration) -> Result<LeaseHandle, ClusterError> {
        // Re-acquire under the same holder. NATS' update CAS is on
        // the JS revision; we read-then-update to get the latest one.
        let entry = self
            .store
            .entry(&lease.name)
            .await
            .map_err(|e| ClusterError::BackendUnavailable {
                reason: format!("nats lease entry `{}`: {e}", lease.name),
            })?
            .ok_or_else(|| ClusterError::CasConflict {
                key: lease.name.clone(),
                reason: "lease not present".to_owned(),
            })?;

        let rec: LeaseRecord =
            serde_json::from_slice(&entry.value).map_err(|e| ClusterError::Internal {
                reason: format!("decode lease record: {e}"),
            })?;
        if rec.holder != lease.holder {
            return Err(ClusterError::CasConflict {
                key: lease.name.clone(),
                reason: "holder mismatch".to_owned(),
            });
        }

        let now = Self::now_secs();
        let expires_at_secs = now + ttl.as_secs().max(1);
        let payload = serde_json::to_vec(&LeaseRecord {
            holder: lease.holder.clone(),
            expires_at_secs,
        })
        .map_err(|e| ClusterError::Internal {
            reason: format!("encode lease record: {e}"),
        })?;
        let new_revision = self
            .store
            .update(&lease.name, payload.into(), entry.revision)
            .await
            .map_err(|e| ClusterError::CasConflict {
                key: lease.name.clone(),
                reason: format!("renew CAS failed: {e}"),
            })?;

        Ok(LeaseHandle {
            name: lease.name.clone(),
            holder: lease.holder.clone(),
            fence: FenceToken(new_revision),
            expires_at: SystemTime::UNIX_EPOCH + Duration::from_secs(expires_at_secs),
        })
    }

    async fn release(&self, lease: &LeaseHandle) -> Result<(), ClusterError> {
        // CAS-check holder before deleting. Mismatch is silently OK
        // (idempotent) — the lease may have already been re-acquired.
        let Some(entry) =
            self.store
                .entry(&lease.name)
                .await
                .map_err(|e| ClusterError::BackendUnavailable {
                    reason: format!("nats lease entry `{}`: {e}", lease.name),
                })?
        else {
            return Ok(());
        };
        let rec: LeaseRecord = match serde_json::from_slice(&entry.value) {
            Ok(r) => r,
            Err(_) => return Ok(()),
        };
        if rec.holder != lease.holder {
            return Ok(());
        }
        // Tombstone via delete; preserves history but removes the live entry.
        self.store
            .delete(&lease.name)
            .await
            .map_err(|e| ClusterError::BackendUnavailable {
                reason: format!("nats lease delete `{}`: {e}", lease.name),
            })?;
        Ok(())
    }

    async fn current_holder(&self, name: &str) -> Result<Option<LeaseHandle>, ClusterError> {
        let Some(entry) =
            self.store
                .entry(name)
                .await
                .map_err(|e| ClusterError::BackendUnavailable {
                    reason: format!("nats lease entry `{name}`: {e}"),
                })?
        else {
            return Ok(None);
        };
        if entry.operation != Operation::Put {
            return Ok(None);
        }
        let rec: LeaseRecord = match serde_json::from_slice(&entry.value) {
            Ok(r) => r,
            Err(_) => return Ok(None),
        };
        let now = Self::now_secs();
        if rec.expires_at_secs <= now {
            return Ok(None);
        }
        Ok(Some(LeaseHandle {
            name: name.to_owned(),
            holder: rec.holder,
            fence: FenceToken(entry.revision),
            expires_at: SystemTime::UNIX_EPOCH + Duration::from_secs(rec.expires_at_secs),
        }))
    }
}
