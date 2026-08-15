//! Lease lifecycle via JetStream KV.
//!
//! Two lease shapes — leadership (waits until available) and lock
//! (returns immediately on contention). Both back onto the same KV
//! bucket; the only difference is whether the acquire path polls +
//! retries on collision.
//!
//! Fencing tokens come out of a second KV bucket — atomic
//! `update`-with-CAS on `fencing/{key}` increments the counter
//! and the post-update revision becomes the token. Operators
//! relying on fencing for safety should pin everything that
//! consumes a token to compare ≥ rather than == (the trait
//! contract says monotonic, not gap-free).
//!
//! # Lease handle lifecycle
//!
//! [`NatsJetStreamLeaseHandle`] is the canonical lease type. It
//! carries:
//!
//! - `Arc<LeaseInner>` — the per-lease state shared between the
//!   trait-method paths and the background renewal task.
//! - An `AbortHandle` for the renewal task; the handle's `Drop`
//!   fires the abort, so dropping the handle ends the renewal
//!   loop deterministically.
//!
//! Two callers consume the same handle type:
//!
//! - **Async path** ([`acquire_async`] / [`try_acquire_async`]) —
//!   returns `Box<NatsJetStreamLeaseHandle>` as a
//!   `Box<dyn ActiveLease>` for the in-process
//!   async trait.
//! - **Sync FFI path** ([`acquire`] / [`try_acquire`]) — leaks
//!   `Box<NatsJetStreamLeaseHandle>` via `Box::into_raw`,
//!   producing a `WatchHandleBox` (`*mut ()`) the host stores as
//!   a `usize`. `lease_drop` reclaims via `Box::from_raw` and the
//!   ensuing `Drop` aborts the renewal task.
//!
//! Both paths share the same renewal task (one per lease) and
//! the same renew / release async cores, so behaviour is
//! identical regardless of which surface the caller used.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_nats::jetstream::kv::Store as KvStore;
use bytes::Bytes;
use mcpg_cluster_api::{ActiveLease, ClusterError};
use mcpg_plugin_protocol::async_trait;
use mcpg_plugin_sdk::ffi::WatchHandleBox;
use serde::{Deserialize, Serialize};
use tokio::runtime::Handle;
use tokio::task::AbortHandle;

const FENCING_KEY_PREFIX: &str = "fencing";
const ACQUIRE_LEADERSHIP_RETRY_MS: u64 = 500;

/// Persistent KV value for an active lease. Stored under
/// `{leases_bucket}/{role_or_lock_key}`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct LeaseRecord {
    pub(crate) node_id: String,
    pub(crate) fencing_token: u64,
    pub(crate) acquired_at: String,
    pub(crate) expires_at: String,
    /// Expiry as Unix epoch seconds — the machine-comparable twin of the
    /// human-readable `expires_at` string. Used by the reclaim path
    /// to decide whether an incumbent lease has lapsed without parsing
    /// the RFC3339 string. `#[serde(default)]` → a record without this
    /// field deserializes to `0`, which `lease_is_expired` treats as
    /// "unknown, not expired" (conservative: never reclaim a record we
    /// can't date).
    #[serde(default)]
    pub(crate) expires_at_unix: u64,
}

/// Inner state shared between the plugin's trait-method paths
/// (`lease_renew` / `lease_release` borrow it via `&LeaseState`)
/// and the background renewal task (which owns a separate
/// `Arc<LeaseInner>` clone). Holding it behind `Arc` decouples
/// the renewal task's lifetime from the LeaseState lifetime — the
/// `AbortHandle` is the cancellation channel.
pub(crate) struct LeaseInner {
    pub(crate) leases: KvStore,
    pub(crate) key: String,
    pub(crate) node_id: String,
    pub(crate) fencing_token: u64,
    pub(crate) ttl: Duration,
    pub(crate) expires_at: Mutex<String>,
}

/// Per-lease handle held by the consumer plugin (sync FFI path)
/// or the in-process async caller (async `ClusterBackend`
/// path). Owns the renewal task's `AbortHandle`, so dropping the
/// handle ends the renewal loop deterministically; the lease's
/// underlying `LeaseInner` is shared with that task via `Arc`,
/// so drop ordering is "abort the task, then the task drops its
/// `Arc`, then `LeaseInner` drops".
///
/// Implements [`mcpg_cluster_api::ActiveLease`] for
/// the in-process async trait. The sync FFI path uses the same
/// struct via `Box::into_raw` / `Box::from_raw` round-trip — see
/// the module docstring for the full lifecycle.
pub struct NatsJetStreamLeaseHandle {
    pub(crate) inner: Arc<LeaseInner>,
    /// Renewal task — aborted when this state drops.
    renewal_abort: AbortHandle,
}

impl Drop for NatsJetStreamLeaseHandle {
    fn drop(&mut self) {
        self.renewal_abort.abort();
    }
}

#[async_trait]
impl ActiveLease for NatsJetStreamLeaseHandle {
    fn fencing_token(&self) -> u64 {
        self.inner.fencing_token
    }

    fn expires_at(&self) -> String {
        self.inner
            .expires_at
            .lock()
            .map(|g| g.clone())
            .unwrap_or_default()
    }

    async fn renew(&self) -> Result<(), ClusterError> {
        renew_inner(&self.inner).await.map(|_| ())
    }

    async fn release(&self) -> Result<(), ClusterError> {
        release_async(&self.inner).await
    }
}

/// Decision the acquire loop makes after a CAS attempt.
enum AcquireOutcome {
    /// Slot was free; we won. Carries the post-create revision.
    Acquired(LeaseRecord, u64),
    /// Slot is held by another node. Caller decides whether to
    /// wait + retry (leadership) or return Err (lock).
    Held,
    /// Backend failure (connection lost, etc.). Caller surfaces
    /// as `BackendUnavailable`.
    BackendError(String),
}

/// Inputs to `acquire`. Bundled so the function arg list stays
/// short — clippy flags >7-arg functions which this is otherwise.
pub(crate) struct AcquireParams {
    pub(crate) leases: KvStore,
    pub(crate) fencing: KvStore,
    pub(crate) key: String,
    pub(crate) node_id: String,
    pub(crate) ttl_ms: u64,
    /// True for leadership (waits until available), false for
    /// lock (returns BackendUnavailable on contention).
    pub(crate) wait: bool,
    /// 1..=99; renewal task sleeps for `ttl × (100 - pct) / 100`
    /// before firing each renew.
    pub(crate) renew_before_expiry_percent: u32,
}

/// Async core for [`acquire`] / [`acquire_async`]. Holds the
/// retry loop; the sync wrapper just `block_on`s it.
///
/// `params.wait`:
///
/// - `true` — leadership semantics. On contention, sleeps
///   [`ACQUIRE_LEADERSHIP_RETRY_MS`] and retries until the slot
///   frees, bounded by ~10×TTL → eventually surfaces `Timeout`.
/// - `false` — lock semantics with the BLOCKING-wait flag. On
///   contention, returns `BackendUnavailable`; reserved for the
///   FFI-sync `acquire_lock` blocking-by-spec path. Most async
///   callers want [`try_acquire_async`] instead.
pub(crate) async fn acquire_async(
    runtime: Handle,
    params: AcquireParams,
) -> Result<NatsJetStreamLeaseHandle, ClusterError> {
    let AcquireParams {
        leases,
        fencing,
        key,
        node_id,
        ttl_ms,
        wait,
        renew_before_expiry_percent,
    } = params;
    if key.trim().is_empty() {
        return Err(ClusterError::InvalidReference {
            message: "lease key must not be empty".into(),
        });
    }
    let ttl = Duration::from_millis(ttl_ms.max(1));
    let now = Instant::now();
    let max_wait = Duration::from_millis(ttl_ms.saturating_mul(3).max(1_000));

    loop {
        // Mint a fresh fencing token for this attempt. If we
        // lose the CAS we waste a token; that's fine —
        // monotonicity is the contract, not gap-freeness.
        let fencing_token = bump_fencing_token(&fencing, &key).await?;
        let (expires_at, expires_at_unix) = expiry_for(ttl);
        let record = LeaseRecord {
            node_id: node_id.clone(),
            fencing_token,
            acquired_at: now_rfc3339(),
            expires_at,
            expires_at_unix,
        };
        let outcome = try_create(&leases, &key, &record, ttl).await;
        match outcome {
            AcquireOutcome::Acquired(rec, _rev) => {
                let handle = spawn_lease_handle(
                    leases.clone(),
                    runtime.clone(),
                    rec,
                    key.clone(),
                    ttl,
                    renew_before_expiry_percent,
                );
                return Ok(handle);
            }
            AcquireOutcome::Held if wait => {
                if now.elapsed() > max_wait * 10 {
                    return Err(ClusterError::Timeout);
                }
                tokio::time::sleep(Duration::from_millis(ACQUIRE_LEADERSHIP_RETRY_MS)).await;
                continue;
            }
            AcquireOutcome::Held => {
                return Err(ClusterError::BackendUnavailable {
                    reason: format!("lease key `{key}` is held by another node"),
                });
            }
            AcquireOutcome::BackendError(reason) => {
                return Err(ClusterError::BackendUnavailable { reason });
            }
        }
    }
}

/// Async core for [`try_acquire`] / [`try_acquire_async`].
/// Single-shot CAS: returns `Ok(Some)` on acquired, `Ok(None)`
/// on contention, `Err` on backend failure. `params.wait` is
/// ignored — the function name implies non-blocking.
pub(crate) async fn try_acquire_async(
    runtime: Handle,
    params: AcquireParams,
) -> Result<Option<NatsJetStreamLeaseHandle>, ClusterError> {
    let AcquireParams {
        leases,
        fencing,
        key,
        node_id,
        ttl_ms,
        wait: _,
        renew_before_expiry_percent,
    } = params;
    if key.trim().is_empty() {
        return Err(ClusterError::InvalidReference {
            message: "lease key must not be empty".into(),
        });
    }
    let ttl = Duration::from_millis(ttl_ms.max(1));
    let fencing_token = bump_fencing_token(&fencing, &key).await?;
    let (expires_at, expires_at_unix) = expiry_for(ttl);
    let record = LeaseRecord {
        node_id: node_id.clone(),
        fencing_token,
        acquired_at: now_rfc3339(),
        expires_at,
        expires_at_unix,
    };
    match try_create(&leases, &key, &record, ttl).await {
        AcquireOutcome::Acquired(rec, _rev) => {
            let handle = spawn_lease_handle(
                leases.clone(),
                runtime.clone(),
                rec,
                key.clone(),
                ttl,
                renew_before_expiry_percent,
            );
            Ok(Some(handle))
        }
        AcquireOutcome::Held => Ok(None),
        AcquireOutcome::BackendError(reason) => Err(ClusterError::BackendUnavailable { reason }),
    }
}

/// Sync FFI wrapper around [`acquire_async`]. Boxes the resulting
/// handle and leaks it into a `WatchHandleBox` for the FFI vtable.
pub(crate) fn acquire(
    runtime: &Handle,
    params: AcquireParams,
) -> Result<(WatchHandleBox, u64, String), ClusterError> {
    let runtime_for_spawn = runtime.clone();
    let handle = runtime.block_on(acquire_async(runtime_for_spawn, params))?;
    Ok(wrap_handle_sync(Box::new(handle)))
}

/// Sync FFI wrapper around [`try_acquire_async`]. Returns
/// `Ok(None)` when the backend reports the slot is held, mirroring
/// the trait contract for the FFI try-variant slot.
pub(crate) fn try_acquire(
    runtime: &Handle,
    params: AcquireParams,
) -> Result<Option<(WatchHandleBox, u64, String)>, ClusterError> {
    let runtime_for_spawn = runtime.clone();
    let handle_opt = runtime.block_on(try_acquire_async(runtime_for_spawn, params))?;
    match handle_opt {
        Some(h) => Ok(Some(wrap_handle_sync(Box::new(h)))),
        None => Ok(None),
    }
}

/// `Box::into_raw` the lease handle and pull out the metadata the
/// FFI vtable returns alongside the opaque pointer. Used by both
/// sync acquire variants.
fn wrap_handle_sync(handle: Box<NatsJetStreamLeaseHandle>) -> (WatchHandleBox, u64, String) {
    let token = handle.inner.fencing_token;
    let expires_at = handle
        .inner
        .expires_at
        .lock()
        .map(|g| g.clone())
        .unwrap_or_default();
    let raw = Box::into_raw(handle) as *mut ();
    (WatchHandleBox(raw), token, expires_at)
}

/// Prefix that distinguishes leadership leases from locks in the
/// shared KV bucket. Keys are stored as
/// `{LEADERSHIP_PREFIX}.{role}` and `{LOCK_PREFIX}.{key}`. The
/// role-enumeration path strips the leadership prefix to recover
/// the operator-visible role name.
pub(crate) const LEADERSHIP_PREFIX: &str = "leadership";
pub(crate) const LOCK_PREFIX: &str = "lock";

/// Enumerate the roles this node currently holds leadership for.
/// Used by `node_info` to populate `ClusterNodeInfo.roles`.
///
/// Reads every key in the leases bucket, decodes the value as a
/// `LeaseRecord`, filters for entries whose `node_id` matches the
/// caller, and extracts the role suffix from `leadership.{role}`.
/// Locks (`lock.{key}`) are skipped — `node_info.roles` is
/// specifically about leadership, not opportunistic locks.
///
/// Returns Ok with an empty Vec when the bucket is empty or
/// unreachable; node_info is observability-grade and operators
/// shouldn't see a hard error if the bucket can't be enumerated
/// at the moment.
pub(crate) async fn enumerate_held_roles(leases: &KvStore, self_node_id: &str) -> Vec<String> {
    use futures::TryStreamExt;

    let mut keys = match leases.keys().await {
        Ok(s) => s,
        Err(e) => {
            tracing::warn!(
                error = %e,
                "nats cluster: enumerate_held_roles failed to open key stream"
            );
            return Vec::new();
        }
    };

    let mut roles: Vec<String> = Vec::new();
    let prefix_dot = format!("{LEADERSHIP_PREFIX}.");
    loop {
        match keys.try_next().await {
            Ok(Some(key)) => {
                let Some(role) = key.strip_prefix(&prefix_dot) else {
                    continue; // it's a lock, not a leadership lease
                };
                let entry = match leases.entry(&key).await {
                    Ok(Some(e)) => e,
                    Ok(None) => continue, // raced with delete
                    Err(_) => continue,   // transient; skip rather than error
                };
                let record: LeaseRecord = match serde_json::from_slice(&entry.value) {
                    Ok(r) => r,
                    Err(_) => continue, // garbled entry — operator wrote into our bucket
                };
                if record.node_id == self_node_id {
                    roles.push(role.to_owned());
                }
            }
            Ok(None) => break,
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    "nats cluster: enumerate_held_roles key-stream error"
                );
                break;
            }
        }
    }
    roles.sort();
    roles
}

/// Borrow the lease handle for a renew / release op without
/// reclaiming the Box. SAFETY contract per call site.
pub(crate) unsafe fn borrow_state(handle: &WatchHandleBox) -> Option<&NatsJetStreamLeaseHandle> {
    if handle.0.is_null() {
        return None;
    }
    // SAFETY: the host's vtable contract: handle was produced by
    // `acquire`, hasn't been dropped, and lives for the duration
    // of this call. The host-side adapter (`NativeLeaseHandle`)
    // holds the pointer until `lease_drop`.
    Some(unsafe { &*(handle.0 as *const NatsJetStreamLeaseHandle) })
}

/// Reclaim the leaked `Box<NatsJetStreamLeaseHandle>` + drop it.
/// Drop fires `renewal_abort` and frees the heap allocation.
/// Idempotent on null.
pub(crate) unsafe fn drop_state(handle: WatchHandleBox) {
    if handle.0.is_null() {
        return;
    }
    // SAFETY: per the host vtable contract, exactly one
    // `lease_drop` per acquire, and the pointer is still valid.
    unsafe {
        let _ = Box::from_raw(handle.0 as *mut NatsJetStreamLeaseHandle);
    }
}

/// Refresh the KV slot for an existing lease. Reads the current
/// record, verifies we still own it (`node_id` matches), CAS-
/// updates with a new `expires_at`. Returns the new RFC3339
/// expiry on success.
pub(crate) fn renew(
    runtime: &Handle,
    state: &NatsJetStreamLeaseHandle,
) -> Result<String, ClusterError> {
    runtime.block_on(renew_inner(&state.inner))
}

/// Async renewal core, shared between the trait method's
/// block_on path and the background renewal task. Reads the
/// current record, verifies ownership, CAS-updates with a
/// refreshed `expires_at`. Caller decides whether to surface
/// the result to the trait-method caller or just log + retry.
async fn renew_inner(inner: &LeaseInner) -> Result<String, ClusterError> {
    let entry =
        inner
            .leases
            .entry(&inner.key)
            .await
            .map_err(|e| ClusterError::BackendUnavailable {
                reason: format!("lease renew read: {e}"),
            })?;
    let entry = match entry {
        Some(e) => e,
        None => return Err(ClusterError::LeaseExpired),
    };
    let record: LeaseRecord = serde_json::from_slice(&entry.value)
        .map_err(|e| crate::error::json_decode_to_cluster("lease_renew", e))?;
    if record.node_id != inner.node_id || record.fencing_token != inner.fencing_token {
        return Err(ClusterError::LeaseExpired);
    }
    let (expires_at, expires_at_unix) = expiry_for(inner.ttl);
    let new_record = LeaseRecord {
        expires_at,
        expires_at_unix,
        ..record
    };
    let new_value = Bytes::from(serde_json::to_vec(&new_record).expect("LeaseRecord serialises"));
    inner
        .leases
        .update(&inner.key, new_value, entry.revision)
        .await
        .map_err(|e| {
            // Pre-condition mismatch on revision = lost the lease
            // to another writer between read and update.
            let s = e.to_string();
            if s.contains("wrong last sequence") || s.contains("revision") {
                ClusterError::LeaseExpired
            } else {
                ClusterError::BackendUnavailable {
                    reason: format!("lease renew update: {s}"),
                }
            }
        })?;
    if let Ok(mut g) = inner.expires_at.lock() {
        g.clone_from(&new_record.expires_at);
    }
    Ok(new_record.expires_at)
}

/// Async core for [`release`]. Idempotent — if the slot is gone
/// (already released, expired, taken by another node) returns
/// Ok per the trait's "release is best-effort" contract.
pub(crate) async fn release_async(inner: &LeaseInner) -> Result<(), ClusterError> {
    let entry =
        inner
            .leases
            .entry(&inner.key)
            .await
            .map_err(|e| ClusterError::BackendUnavailable {
                reason: format!("lease release read: {e}"),
            })?;
    let entry = match entry {
        Some(e) => e,
        None => return Ok(()), // already gone
    };
    let record: LeaseRecord = match serde_json::from_slice(&entry.value) {
        Ok(r) => r,
        // Garbled entry (operator wrote into our bucket) — let
        // it stay; we don't own it.
        Err(_) => return Ok(()),
    };
    if record.node_id != inner.node_id || record.fencing_token != inner.fencing_token {
        // Lease has been re-acquired by someone else; nothing
        // for us to release.
        return Ok(());
    }
    inner
        .leases
        .delete(&inner.key)
        .await
        .map_err(|e| ClusterError::BackendUnavailable {
            reason: format!("lease release delete: {e}"),
        })?;
    Ok(())
}

/// Sync FFI wrapper around [`release_async`].
pub(crate) fn release(
    runtime: &Handle,
    state: &NatsJetStreamLeaseHandle,
) -> Result<(), ClusterError> {
    runtime.block_on(release_async(&state.inner))
}

async fn bump_fencing_token(fencing: &KvStore, key: &str) -> Result<u64, ClusterError> {
    let path = format!("{FENCING_KEY_PREFIX}.{key}");
    // Loop the CAS until we land a successful update; bounded so a
    // pathological contender can't spin us forever.
    for _ in 0..16 {
        let entry = fencing
            .entry(&path)
            .await
            .map_err(|e| ClusterError::BackendUnavailable {
                reason: format!("fencing read: {e}"),
            })?;
        let next: u64 = match &entry {
            Some(e) => {
                let s = std::str::from_utf8(&e.value).unwrap_or("0");
                s.parse::<u64>().unwrap_or(0).saturating_add(1)
            }
            None => 1,
        };
        let value = Bytes::from(next.to_string().into_bytes());
        // Convert both arms to a common `Result<(), String>` so
        // the borrow checker sees one error type. The two upstream
        // futures return `Error<UpdateErrorKind>` and
        // `Error<CreateErrorKind>` respectively; we don't need the
        // typed shape — we pivot on the message text.
        let result: Result<(), String> = match entry {
            Some(e) => fencing
                .update(&path, value, e.revision)
                .await
                .map(|_| ())
                .map_err(|e| e.to_string()),
            None => fencing
                .create(&path, value)
                .await
                .map(|_| ())
                .map_err(|e| e.to_string()),
        };
        match result {
            Ok(()) => return Ok(next),
            Err(s) => {
                if s.contains("wrong last sequence")
                    || s.contains("revision")
                    || s.contains("already exists")
                {
                    // Lost the CAS race; retry with the fresh
                    // sequence. Fencing tokens MAY skip values —
                    // the trait only requires monotonicity.
                    continue;
                }
                return Err(ClusterError::BackendUnavailable {
                    reason: format!("fencing CAS: {s}"),
                });
            }
        }
    }
    Err(ClusterError::BackendUnavailable {
        reason: "fencing CAS exhausted retries".into(),
    })
}

async fn try_create(
    leases: &KvStore,
    key: &str,
    record: &LeaseRecord,
    _ttl: Duration,
) -> AcquireOutcome {
    let value = Bytes::from(serde_json::to_vec(record).expect("LeaseRecord serialises"));
    match leases.create(key, value.clone()).await {
        Ok(rev) => AcquireOutcome::Acquired(record.clone(), rev),
        Err(e) => {
            let s = e.to_string();
            // async-nats reports key-already-exists as a "wrong
            // last sequence" error on `create`. The slot is occupied.
            if s.contains("wrong last sequence")
                || s.contains("already exists")
                || s.contains("constraint")
            {
                // NATS JetStream KV has no per-key TTL, so a holder
                // that crashed without `release` would otherwise wedge
                // this slot forever (every peer's `create` keeps seeing
                // the stale key). Reclaim an EXPIRED incumbent via a
                // CAS-replace; a live one is genuinely Held.
                reclaim_if_expired(leases, key, record, value).await
            } else {
                AcquireOutcome::BackendError(format!("kv create: {s}"))
            }
        }
    }
}

/// On a `create` collision, read the incumbent lease record; if it has
/// lapsed ([`lease_is_expired`]), CAS-replace it with ours to reclaim a
/// crashed holder's slot. Our record carries a fresh, strictly-higher
/// fencing token (minted by the caller before this attempt), so a
/// resurrected dead holder writing with its old, lower token is still
/// fenced out — reclamation is split-brain-safe. Anything else (live
/// incumbent, unparseable record, lost CAS race, vanished key) → `Held`.
async fn reclaim_if_expired(
    leases: &KvStore,
    key: &str,
    record: &LeaseRecord,
    value: Bytes,
) -> AcquireOutcome {
    let entry = match leases.entry(key).await {
        Ok(Some(e)) => e,
        // Key vanished between the failed create and this read (released
        // concurrently) — treat as Held for this attempt; the blocking
        // caller retries and will `create` cleanly next pass.
        Ok(None) => return AcquireOutcome::Held,
        Err(e) => return AcquireOutcome::BackendError(format!("lease reclaim read: {e}")),
    };
    let incumbent: LeaseRecord = match serde_json::from_slice(&entry.value) {
        Ok(r) => r,
        // Don't blindly stomp a record we can't parse.
        Err(_) => return AcquireOutcome::Held,
    };
    if !lease_is_expired(&incumbent) {
        return AcquireOutcome::Held;
    }
    // Expired → take over via CAS on the revision we just read. If the
    // CAS loses (another node reclaimed first), it's Held this attempt.
    match leases.update(key, value, entry.revision).await {
        Ok(rev) => {
            tracing::info!(
                key = %key,
                evicted_node = %incumbent.node_id,
                evicted_token = incumbent.fencing_token,
                new_token = record.fencing_token,
                "nats cluster: reclaimed an expired lease from a departed holder"
            );
            AcquireOutcome::Acquired(record.clone(), rev)
        }
        Err(_) => AcquireOutcome::Held,
    }
}

fn spawn_lease_handle(
    leases: KvStore,
    runtime: Handle,
    record: LeaseRecord,
    key: String,
    ttl: Duration,
    renew_before_expiry_percent: u32,
) -> NatsJetStreamLeaseHandle {
    let inner = Arc::new(LeaseInner {
        leases,
        key,
        node_id: record.node_id.clone(),
        fencing_token: record.fencing_token,
        ttl,
        expires_at: Mutex::new(record.expires_at.clone()),
    });
    let renewal_inner = Arc::clone(&inner);
    let join = runtime
        .spawn(async move { renewal_loop(renewal_inner, renew_before_expiry_percent).await });
    NatsJetStreamLeaseHandle {
        inner,
        renewal_abort: join.abort_handle(),
    }
}

/// Per-lease background renewal task. Sleeps for
/// `ttl × renew_before_expiry_percent / 100`, then issues a
/// CAS-update on the KV slot. On success, sleeps again for the
/// same interval (the new expiry is `now + ttl` so the next
/// renewal lands at the same fraction of the next TTL window).
///
/// Failure modes:
/// - `LeaseExpired` (CAS mismatch / record gone) → the lease is
///   unrecoverable. Log + return; the task ends. The host's
///   `NativeLeaseHandle` will surface the next manual `renew`
///   call's failure to the caller.
/// - `BackendUnavailable` (NATS / JS hiccup) → log + retry on
///   the next tick. Operators see the warn log; the next tick
///   either succeeds (transient blip resolved) or hits a real
///   permanent failure that escalates to LeaseExpired.
async fn renewal_loop(inner: Arc<LeaseInner>, renew_before_expiry_percent: u32) {
    // Clamp to 1..=99 even though config.validate enforces this.
    let pct = renew_before_expiry_percent.clamp(1, 99);
    let sleep_ratio = (100 - pct) as u64;
    // sleep = ttl * (100 - pct) / 100 — the wait BEFORE renewing.
    // Example: pct=50, ttl=30s → sleep 15s, then renew.
    let sleep_for = std::cmp::max(
        Duration::from_millis(inner.ttl.as_millis() as u64 * sleep_ratio / 100),
        Duration::from_millis(50), // floor to avoid pathological zero-sleep
    );
    loop {
        tokio::time::sleep(sleep_for).await;
        match renew_inner(&inner).await {
            Ok(new_expiry) => {
                tracing::debug!(
                    key = %inner.key,
                    fencing_token = inner.fencing_token,
                    new_expiry = %new_expiry,
                    "nats cluster: lease auto-renewed"
                );
            }
            Err(ClusterError::LeaseExpired) => {
                tracing::warn!(
                    key = %inner.key,
                    fencing_token = inner.fencing_token,
                    "nats cluster: lease auto-renewal hit LeaseExpired — \
                     another node now owns this slot. Renewal task ending; \
                     next host-driven `lease_renew` will surface the failure."
                );
                return;
            }
            Err(other) => {
                tracing::warn!(
                    key = %inner.key,
                    error = %format!("{other:?}"),
                    "nats cluster: lease auto-renewal failed; will retry next tick"
                );
            }
        }
    }
}

fn now_rfc3339() -> String {
    rfc3339_after(Duration::ZERO)
}

fn now_unix_secs() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

/// The expiry pair (`expires_at` RFC3339 string + `expires_at_unix`
/// epoch seconds) for a lease minted/renewed `ttl` from now. Keeping
/// both in lockstep is the single source for record construction.
fn expiry_for(ttl: Duration) -> (String, u64) {
    (
        rfc3339_after(ttl),
        now_unix_secs().saturating_add(ttl.as_secs()),
    )
}

/// Whether an incumbent lease record has lapsed (its holder almost
/// certainly crashed without releasing). `expires_at_unix == 0` means
/// the record predates the field — treat as NOT expired so we never
/// stomp a lease we can't date.
fn lease_is_expired(record: &LeaseRecord) -> bool {
    record.expires_at_unix != 0 && record.expires_at_unix <= now_unix_secs()
}

fn rfc3339_after(offset: Duration) -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let secs = (SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        + offset)
        .as_secs() as i64;
    let (y, m, d, h, mn, s) = crate::epoch_to_ymdhms(secs);
    format!("{y:04}-{m:02}-{d:02}T{h:02}:{mn:02}:{s:02}Z")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rec(expires_at_unix: u64) -> LeaseRecord {
        LeaseRecord {
            node_id: "n1".into(),
            fencing_token: 1,
            acquired_at: now_rfc3339(),
            expires_at: rfc3339_after(Duration::ZERO),
            expires_at_unix,
        }
    }

    #[test]
    fn lease_is_expired_only_when_past_and_dated() {
        // A lapsed record is reclaimable; a future one is held.
        assert!(lease_is_expired(&rec(now_unix_secs().saturating_sub(5))));
        assert!(!lease_is_expired(&rec(now_unix_secs() + 60)));
        // expires_at_unix == 0 means "predates the field" → conservative
        // NOT-expired so we never stomp an undatable record.
        assert!(!lease_is_expired(&rec(0)));
    }

    #[test]
    fn expiry_for_is_in_the_future_and_consistent() {
        let before = now_unix_secs();
        let (s, unix) = expiry_for(Duration::from_secs(30));
        assert!(
            unix >= before + 30 && unix <= now_unix_secs() + 31,
            "unix={unix}"
        );
        assert!(s.ends_with('Z') && s.contains('T'), "rfc3339: {s}");
    }
}
