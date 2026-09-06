//! NATS connect + JetStream bootstrap.
//!
//! Connects to the operator's NATS cluster (single URL or list),
//! probes JetStream availability, and idempotently creates the
//! KV buckets the rest of the plugin relies on:
//!
//! - `{leases_bucket}` — leadership + lock records (TTL per-key)
//! - `{fencing_bucket}` — monotonic counters keyed on lease/lock
//!   name; incremented with CAS to mint fencing tokens
//!
//! Stream creation for pub/sub fan-out is lazy — a plugin run that
//! never subscribes doesn't spin up a stream.

use std::time::Duration;

use async_nats::Client as NatsClient;
use async_nats::ConnectOptions;
use async_nats::jetstream::{Context as JsContext, kv::Store as KvStore};
use mcpg_cluster_api::ClusterError;

use crate::config::{AuthConfig, ClusterNatsConfig, KvStorage};

/// Live connection + bootstrapped buckets. Cheap to clone —
/// async-nats `Client` is itself an `Arc<Inner>` upstream.
#[derive(Clone)]
pub(crate) struct NatsClientHandle {
    pub(crate) nats: NatsClient,
    /// KV bucket holding lease records.
    pub(crate) leases: KvStore,
    /// KV bucket holding fencing-token counters.
    pub(crate) fencing: KvStore,
    /// KV bucket exposed via the `KeyValueStore` primitive accessor.
    /// Backs the gateway's session / pipeline / task / subscription
    /// stores when operators bind `cluster: { kind: nats }` and skip
    /// per-capability overrides.
    pub(crate) state: KvStore,
}

impl NatsClientHandle {
    pub(crate) async fn connect(cfg: &ClusterNatsConfig) -> Result<Self, ClusterError> {
        let opts = build_connect_options(cfg)
            .await
            .map_err(|reason| ClusterError::BackendUnavailable { reason })?;
        let server_str = cfg.servers.join(",");

        let nats = tokio::time::timeout(
            Duration::from_millis(cfg.connection.connect_timeout_ms),
            opts.connect(server_str),
        )
        .await
        .map_err(|_| ClusterError::BackendUnavailable {
            reason: format!(
                "nats connect: timeout after {}ms",
                cfg.connection.connect_timeout_ms
            ),
        })?
        .map_err(|e| ClusterError::BackendUnavailable {
            reason: format!("nats connect: {e}"),
        })?;

        let js = if let Some(domain) = cfg.jetstream.domain.as_ref().filter(|s| !s.is_empty()) {
            async_nats::jetstream::with_domain(nats.clone(), domain.clone())
        } else {
            async_nats::jetstream::new(nats.clone())
        };

        // Idempotent KV bucket creation. async-nats' `create_key_value`
        // returns the existing bucket if it matches; we treat
        // mismatched-config errors as a register-time failure so an
        // operator who renamed a bucket post-deploy sees the error
        // immediately, not after a lease miss.
        let storage = match cfg.jetstream.storage {
            KvStorage::File => async_nats::jetstream::stream::StorageType::File,
            KvStorage::Memory => async_nats::jetstream::stream::StorageType::Memory,
        };
        let leases = ensure_kv_bucket(
            &js,
            &cfg.jetstream.leases_bucket,
            cfg.jetstream.replicas,
            // Lease records are bounded — operators rarely run more
            // than a handful of named roles + locks. 1MB cap is generous.
            1024 * 1024,
            storage,
        )
        .await?;
        let fencing = ensure_kv_bucket(
            &js,
            &cfg.jetstream.fencing_bucket,
            cfg.jetstream.replicas,
            64 * 1024,
            storage,
        )
        .await?;
        // Bucket the `KeyValueStore` primitive accessor hands out.
        // 64 MiB cap matches the cluster.redis primitive's
        // sizing default; capabilities that need more set a per-cap
        // override pointing at a dedicated bucket.
        let state = ensure_kv_bucket(
            &js,
            &cfg.jetstream.state_bucket,
            cfg.jetstream.replicas,
            64 * 1024 * 1024,
            storage,
        )
        .await?;

        Ok(Self {
            nats,
            leases,
            fencing,
            state,
        })
    }
}

async fn build_connect_options(cfg: &ClusterNatsConfig) -> Result<ConnectOptions, String> {
    // rustls needs a process-default CryptoProvider before the first
    // `ClientConfig::builder()` call (ours for an inline CA, async-nats'
    // internal one on any TLS upgrade); the dep graph compiles in both
    // `ring` and `aws-lc-rs`, so it cannot auto-pick one and panics
    // instead. A cdylib's rustls statics belong to its own linkage unit
    // — the host binary's installer can't reach them — so install here,
    // Once-guarded; if some other code in this linkage installed first,
    // that one wins.
    {
        static TLS_PROVIDER: std::sync::Once = std::sync::Once::new();
        TLS_PROVIDER.call_once(|| {
            let _ = async_nats::rustls::crypto::ring::default_provider().install_default();
        });
    }

    let mut opts = ConnectOptions::new()
        .name(format!(
            "mcpg-cluster-nats-jetstream/{} ({})",
            env!("CARGO_PKG_VERSION"),
            cfg.node.id
        ))
        .connection_timeout(Duration::from_millis(cfg.connection.connect_timeout_ms))
        .request_timeout(Some(Duration::from_millis(
            cfg.connection.operation_timeout_ms,
        )));

    if let Some(auth) = &cfg.auth {
        opts = match auth {
            AuthConfig::Token { token } => opts.token(token.clone()),
            AuthConfig::UserPassword { user, password } => {
                opts.user_and_password(user.clone(), password.clone())
            }
            AuthConfig::CredentialsFile { path } => opts
                .credentials_file(path)
                .await
                .map_err(|e| format!("nats credentials_file {path}: {e}"))?,
            // Inline credentials-file content. async-nats parses the
            // JWT + nkey seed out of the blob the same way it does for
            // a file. Its parse errors are static strings, so the
            // mapped message never carries the credential itself.
            AuthConfig::Credentials { creds } => opts
                .credentials(creds)
                .map_err(|e| format!("nats auth.creds (inline credentials): {e}"))?,
        };
    }

    // Secure-by-default: require TLS unless the operator explicitly
    // opts out via `tls: { require_tls: false }`. This holds even when the
    // `tls` block is omitted. async-nats `require_tls` only sets
    // `tls_required`; it exposes no server-cert-verification toggle, so
    // whenever TLS is negotiated it is always rustls-verified.
    let mut require_tls = true;
    if let Some(tls) = &cfg.tls {
        if let Some(ca) = &tls.ca_cert {
            // `ca_cert` is either inline PEM or a filesystem path
            // (detected by the `-----BEGIN` prefix, like the redis
            // coordinator). Either way the custom root becomes
            // the trust root in place of the system roots.
            opts = if crate::config::is_inline_pem(ca) {
                opts.tls_client_config(inline_ca_client_config(ca)?)
            } else {
                opts.add_root_certificates(ca.into())
            };
        }
        require_tls = tls.require_tls;
    }
    opts = opts.require_tls(require_tls);

    Ok(opts)
}

/// Build the rustls client config for an inline `tls.ca_cert` PEM.
///
/// async-nats' root-cert API takes only filesystem paths, and it
/// re-reads them on every (re)connect — a temp file would have to
/// outlive the process (and 0600 perms don't port to Windows). Handing
/// async-nats a ready `ClientConfig` keeps the PEM off disk entirely
/// and survives reconnects (the config is cloned per attempt). The
/// build mirrors async-nats' own path-based one: the supplied roots
/// only (no system roots), server-cert verification on, no client auth.
fn inline_ca_client_config(pem: &str) -> Result<async_nats::rustls::ClientConfig, String> {
    use async_nats::rustls;
    use rustls::pki_types::CertificateDer;
    use rustls::pki_types::pem::PemObject;

    let certs = CertificateDer::pem_slice_iter(pem.as_bytes())
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("nats tls.ca_cert: invalid inline PEM: {e}"))?;
    if certs.is_empty() {
        return Err(
            "nats tls.ca_cert: inline PEM holds no CERTIFICATE section (is it a key or \
             credentials blob?)"
                .to_string(),
        );
    }
    let total = certs.len();
    let mut roots = rustls::RootCertStore::empty();
    let (added, ignored) = roots.add_parsable_certificates(certs);
    if added == 0 || ignored > 0 {
        return Err(format!(
            "nats tls.ca_cert: inline PEM: {added} of {total} certificate(s) usable as a \
             trust root, {ignored} rejected"
        ));
    }
    Ok(rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth())
}

async fn ensure_kv_bucket(
    js: &JsContext,
    name: &str,
    replicas: u32,
    max_bytes: i64,
    storage: async_nats::jetstream::stream::StorageType,
) -> Result<KvStore, ClusterError> {
    use async_nats::jetstream::kv::Config as KvConfig;

    // Fast path: bucket already exists. We reuse it as-is — the
    // storage backend is fixed at create time and JS doesn't
    // support migrating between memory + file in place. If the
    // pre-existing bucket has a different storage backend than
    // the operator now requests, that's a deliberate operator
    // choice; we honour it without erroring.
    if let Ok(store) = js.get_key_value(name).await {
        return Ok(store);
    }

    let make_config = |reps: u32| KvConfig {
        bucket: name.to_string(),
        max_bytes,
        num_replicas: reps as usize,
        // File storage by default per `KvStorage::File`. Operators
        // pick `memory` when (a) they don't need durability across
        // NATS restarts or (b) the NATS pod doesn't have a
        // writable `--store_dir` (common in containerised dev).
        storage,
        ..Default::default()
    };

    match js.create_key_value(make_config(replicas)).await {
        Ok(store) => Ok(store),
        Err(e) if replicas > 1 && looks_like_insufficient_peers(&e.to_string()) => {
            // Operator configured `replicas: N > 1` but the cluster
            // doesn't have N peers. Common in dev (single-node
            // NATS) and during cluster bring-up. Warn loudly + fall
            // back to single-replica so the gateway boots; the
            // operator sees the warn line in the startup log and
            // can decide whether to fix the cluster size or accept
            // the degraded durability.
            tracing::warn!(
                bucket = name,
                requested_replicas = replicas,
                "nats cluster: requested replicas > available peers — falling \
                 back to replicas: 1. Either provision a multi-node NATS \
                 cluster or set `jetstream.replicas: 1` to silence this warning."
            );
            js.create_key_value(make_config(1)).await.map_err(|e| {
                ClusterError::BackendUnavailable {
                    reason: format!("nats kv ensure_bucket {name} (after replica fallback): {e}"),
                }
            })
        }
        Err(e) => Err(ClusterError::BackendUnavailable {
            reason: format!("nats kv ensure_bucket {name}: {e}"),
        }),
    }
}

/// Detect the JS error pattern that indicates the cluster doesn't
/// have enough peers to satisfy the requested replica count.
/// async-nats wraps the upstream JS error in a `String`-able Error;
/// the message text is the most stable surface to pivot on.
fn looks_like_insufficient_peers(msg: &str) -> bool {
    let m = msg.to_ascii_lowercase();
    m.contains("insufficient replicas")
        || m.contains("no suitable peers")
        || m.contains("not enough peers")
        || m.contains("replicas") && (m.contains("not") || m.contains("insufficient"))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Static PEM for the inline-CA tests. Only parsing is exercised —
    /// no handshake — so expiry is irrelevant.
    const TEST_CA_PEM: &str = "-----BEGIN CERTIFICATE-----\n\
MIIBhDCCASmgAwIBAgIUfIlbzDivcYV5JW8aXnt92hTShw0wCgYIKoZIzj0EAwIw\n\
FzEVMBMGA1UEAwwMbWNwZy10ZXN0LWNhMB4XDTI2MDkwNDIwMzk1N1oXDTM2MDkw\n\
MTIwMzk1N1owFzEVMBMGA1UEAwwMbWNwZy10ZXN0LWNhMFkwEwYHKoZIzj0CAQYI\n\
KoZIzj0DAQcDQgAEmuOrY8CLXKq8f3BGgSeugoQtwMfkzdztxUX8gRbjXlAdo9CP\n\
Q6mPxFwwVJa6vkSUagc4XMFTwPR0XKXnyRSeYqNTMFEwHQYDVR0OBBYEFIkT1jxH\n\
2Q1g1AuYGYCNCSn2ItU2MB8GA1UdIwQYMBaAFIkT1jxH2Q1g1AuYGYCNCSn2ItU2\n\
MA8GA1UdEwEB/wQFMAMBAf8wCgYIKoZIzj0EAwIDSQAwRgIhAL5syMJr7h2NAxJW\n\
Os4yEYf81h/OzOGhG6SHRFnkeaiSAiEAjxmd81w+EdoDmzCJrBrZBFklpK4K5rkb\n\
sEkUiIkzAWo=\n\
-----END CERTIFICATE-----\n";

    fn cfg_with(extra: serde_json::Value) -> ClusterNatsConfig {
        let mut base = serde_json::json!({
            "servers": ["tls://nats:4222"],
            "node": {"id": "g1"}
        });
        base.as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        ClusterNatsConfig::parse(&base.to_string()).unwrap()
    }

    fn valid_creds() -> String {
        [
            "-----BEGIN NATS USER JWT-----",
            "eyJ0eXAiOiJKV1QiLCJhbGciOiJlZDI1NTE5LW5rZXkifQ.eyJzdWIiOiJVQUJDIn0.c2ln",
            "------END NATS USER JWT------",
            "",
            "-----BEGIN USER NKEY SEED-----",
            "SUACH75SWCM5D2JMJM6EKLR2WDARVGZT4QC6LX3AGHSWOMVAKERABBBRWM",
            "------END USER NKEY SEED------",
            "",
        ]
        .join("\n")
    }

    #[tokio::test]
    async fn inline_credentials_build_connect_options() {
        let cfg = cfg_with(serde_json::json!({
            "auth": {"method": "credentials", "creds": valid_creds()}
        }));
        build_connect_options(&cfg)
            .await
            .expect("inline creds wire into ConnectOptions");
    }

    #[tokio::test]
    async fn inline_credentials_bad_seed_fails_construction_without_echo() {
        // Passes the config-load shape check (both sections present)
        // but carries a seed with a broken checksum — async-nats
        // rejects it while the connect options are built.
        let creds = valid_creds().replace(
            "SUACH75SWCM5D2JMJM6EKLR2WDARVGZT4QC6LX3AGHSWOMVAKERABBBRWM",
            "SUABADBADBADBADBADBADBADBADBADBADBADBADBADBADBADBADBADBAD",
        );
        let cfg = cfg_with(serde_json::json!({
            "auth": {"method": "credentials", "creds": creds}
        }));
        let err = build_connect_options(&cfg).await.unwrap_err();
        assert!(err.contains("auth.creds"), "{err}");
        assert!(!err.contains("SUABAD"), "seed echoed in error: {err}");
    }

    #[tokio::test]
    async fn credentials_file_variant_still_reads_a_file() {
        // The file path route stays on `credentials_file`, which reads
        // eagerly — a missing file fails construction with a
        // path-naming error, not an inline-creds one.
        let cfg = cfg_with(serde_json::json!({
            "auth": {"method": "credentials_file", "path": "/no/such/nats.creds"}
        }));
        let err = build_connect_options(&cfg).await.unwrap_err();
        assert!(err.contains("credentials_file"), "{err}");
        assert!(err.contains("/no/such/nats.creds"), "{err}");
    }

    #[tokio::test]
    async fn token_auth_still_builds() {
        let cfg = cfg_with(serde_json::json!({
            "auth": {"method": "token", "token": "t"}
        }));
        build_connect_options(&cfg).await.unwrap();
    }

    #[tokio::test]
    async fn inline_ca_cert_builds_tls_client_config() {
        let cfg = cfg_with(serde_json::json!({
            "tls": {"ca_cert": TEST_CA_PEM}
        }));
        let opts = build_connect_options(&cfg).await.unwrap();
        // The inline route hands async-nats a ready ClientConfig; no
        // certificate PATH may be registered (a PEM string treated as
        // a path would fail at every TLS upgrade).
        assert!(!format!("{opts:?}").contains("BEGIN CERTIFICATE"));
    }

    #[tokio::test]
    async fn inline_ca_cert_invalid_pem_rejected() {
        let cfg = cfg_with(serde_json::json!({
            "tls": {"ca_cert": "-----BEGIN CERTIFICATE-----\nnot base64 !!\n-----END CERTIFICATE-----\n"}
        }));
        let err = build_connect_options(&cfg).await.unwrap_err();
        assert!(err.contains("tls.ca_cert"), "{err}");
    }

    #[tokio::test]
    async fn inline_ca_cert_without_certificate_section_rejected() {
        // Inline PEM that is not a certificate (e.g. a mispasted key)
        // is caught at construction.
        let cfg = cfg_with(serde_json::json!({
            "tls": {"ca_cert": "-----BEGIN PRIVATE KEY-----\nMAA=\n-----END PRIVATE KEY-----\n"}
        }));
        let err = build_connect_options(&cfg).await.unwrap_err();
        assert!(err.contains("no CERTIFICATE section"), "{err}");
    }

    #[tokio::test]
    async fn path_ca_cert_still_routes_to_the_path_api() {
        // A path value is handed to async-nats untouched and is only
        // read at (re)connect, so a not-yet-mounted file does not fail
        // construction — the path must show up in the options.
        let cfg = cfg_with(serde_json::json!({
            "tls": {"ca_cert": "/no/such/ca.pem"}
        }));
        let opts = build_connect_options(&cfg).await.unwrap();
        assert!(format!("{opts:?}").contains("/no/such/ca.pem"));
    }
}
