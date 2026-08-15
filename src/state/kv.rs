use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_nats::jetstream::kv::Store as KvStore;
use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use mcpg_cluster_api::{ClusterError, Entry, KeyValueStore};

/// JetStream KV-backed state.
///
/// One `NatsKv` instance == one KV bucket. Multiple capabilities
/// pointing at the same bucket share the underlying NATS connection
/// only if operators explicitly arrange it (the type itself opens
/// a fresh connection per `connect`).
#[derive(Debug)]
pub struct NatsKv {
    store: KvStore,
}

impl NatsKv {
    /// Construct a `NatsKv` over an already-bootstrapped JS KV store.
    /// Used by `mcpg-plugin-cluster-nats` to share its single
    /// connection across the coordinator + the four primitive
    /// accessors instead of opening a fresh connection per primitive.
    pub fn with_store(store: KvStore) -> Self {
        Self { store }
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
/// one consul/etcd/redis satisfy natively), a value written with a TTL
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

#[async_trait]
impl KeyValueStore for NatsKv {
    async fn get(&self, key: &str) -> Result<Option<Entry>, ClusterError> {
        // Per-key TTL rides in the value envelope (JetStream KV has only a
        // bucket-wide max_age); decode it and surface `expires_at`. A
        // logically-expired record reads as absent — the bucket max_age
        // reaps the physical key in the background.
        match self.store.get(&encode_key(key)).await {
            Ok(Some(stored)) => {
                let (bytes, expires_at_secs) = decode_value(stored);
                if is_expired(expires_at_secs) {
                    return Ok(None);
                }
                Ok(Some(Entry {
                    bytes,
                    expires_at: expires_at_secs.map(|s| UNIX_EPOCH + Duration::from_secs(s)),
                }))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(ClusterError::BackendUnavailable {
                reason: format!("nats kv get `{key}`: {e}"),
            }),
        }
    }

    async fn put(
        &self,
        key: &str,
        value: Bytes,
        ttl: Option<Duration>,
    ) -> Result<(), ClusterError> {
        // Per-key TTL rides in the value envelope (JetStream KV has only a
        // bucket-wide max_age); no TTL → the value is stored verbatim. The
        // bucket-wide max_age still handles bulk reaping of stale records.
        let stored = encode_value(&value, expires_at_from(ttl));
        self.store
            .put(encode_key(key), stored)
            .await
            .map(|_| ())
            .map_err(|e| ClusterError::BackendUnavailable {
                reason: format!("nats kv put `{key}`: {e}"),
            })
    }

    async fn put_if_absent(
        &self,
        key: &str,
        value: Bytes,
        ttl: Option<Duration>,
    ) -> Result<bool, ClusterError> {
        // JetStream KV `create` is the atomic put-if-absent: it succeeds
        // only when the key does not already exist, otherwise it errors
        // ("wrong last sequence" / "already exists"). Map that to the
        // bool contract. Per-key TTL rides in the value envelope; the
        // bucket-wide max_age reaps physically-stale keys. NB: `create`
        // sees PHYSICAL presence, so a logically-expired-but-present
        // record still loses here even though `get` reports it absent —
        // the one backend where "expired == absent" doesn't hold for
        // put_if_absent.
        let stored = encode_value(&value, expires_at_from(ttl));
        match self.store.create(encode_key(key), stored).await {
            Ok(_rev) => Ok(true),
            Err(e) => {
                let s = e.to_string();
                if s.contains("wrong last sequence")
                    || s.contains("already exists")
                    || s.contains("key exists")
                {
                    Ok(false)
                } else {
                    Err(ClusterError::BackendUnavailable {
                        reason: format!("nats kv create `{key}`: {s}"),
                    })
                }
            }
        }
    }

    async fn delete(&self, key: &str) -> Result<bool, ClusterError> {
        // KV `delete` is idempotent — succeeds even when the key
        // doesn't exist. To match the contract (return false when
        // the key was missing), check existence first.
        let encoded = encode_key(key);
        let existed = self.store.get(&encoded).await.is_ok_and(|v| v.is_some());
        self.store
            .delete(&encoded)
            .await
            .map_err(|e| ClusterError::BackendUnavailable {
                reason: format!("nats kv delete `{key}`: {e}"),
            })?;
        Ok(existed)
    }

    async fn list_prefix(
        &self,
        prefix: &str,
        limit: usize,
    ) -> Result<Vec<(String, Entry)>, ClusterError> {
        let mut keys_stream =
            self.store
                .keys()
                .await
                .map_err(|e| ClusterError::BackendUnavailable {
                    reason: format!("nats kv keys: {e}"),
                })?;
        // Keys come back ENCODED. The escape is prefix-preserving, so
        // encode the query prefix and prefix-match in encoded space, then
        // decode each matched key back to the gateway's original key.
        let encoded_prefix = encode_key(prefix);
        let mut out = Vec::new();
        while let Some(key) = keys_stream.next().await {
            let key = key.map_err(|e| ClusterError::BackendUnavailable {
                reason: format!("nats kv keys iter: {e}"),
            })?;
            if !key.starts_with(&encoded_prefix) {
                continue;
            }
            let Some(decoded) = decode_key(&key) else {
                continue;
            };
            if let Ok(Some(stored)) = self.store.get(&key).await {
                let (bytes, expires_at_secs) = decode_value(stored);
                // Skip logically-expired records (inline-TTL envelope).
                if is_expired(expires_at_secs) {
                    continue;
                }
                out.push((
                    decoded,
                    Entry {
                        bytes,
                        expires_at: expires_at_secs.map(|s| UNIX_EPOCH + Duration::from_secs(s)),
                    },
                ));
                if out.len() >= limit {
                    break;
                }
            }
        }
        Ok(out)
    }

    async fn expire(&self, key: &str, ttl: Option<Duration>) -> Result<bool, ClusterError> {
        // JetStream KV has no native per-key TTL, so "set the TTL" means
        // re-writing the value with a fresh inline-expiry envelope. The
        // value is preserved; only the expiry changes (`ttl: None` clears
        // it). Returns false when the key is physically absent.
        let encoded = encode_key(key);
        let current =
            self.store
                .get(&encoded)
                .await
                .map_err(|e| ClusterError::BackendUnavailable {
                    reason: format!("nats kv expire get `{key}`: {e}"),
                })?;
        let Some(stored) = current else {
            return Ok(false);
        };
        let (value, _) = decode_value(stored);
        let restored = encode_value(&value, expires_at_from(ttl));
        self.store
            .put(&encoded, restored)
            .await
            .map_err(|e| ClusterError::BackendUnavailable {
                reason: format!("nats kv expire put `{key}`: {e}"),
            })?;
        Ok(true)
    }
}

// Integration tests against a real NATS server live in the
// gateway-level suite; pure-impl tests here would need a
// feature-gated nats fixture, which isn't wired up.

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
