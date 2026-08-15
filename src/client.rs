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
            opts = opts.add_root_certificates(ca.into());
        }
        require_tls = tls.require_tls;
    }
    opts = opts.require_tls(require_tls);

    Ok(opts)
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
