//! `dev.mcpg.cluster.nats` — NATS JetStream
//! `cluster` plugin.
//!
//! The crate maps the `ClusterBackend`
//! surface onto NATS JetStream as follows:
//!
//! - `node_info` + `list_peers` — driven by the peer heartbeat
//!   publisher / subscriber tasks (see `peer.rs`).
//! - Lease ops (`acquire_leadership`, `acquire_lock`,
//!   `lease_renew`, `lease_release`, `lease_drop`) via JS KV
//!   CAS, with the lease handle passed back to the host as a
//!   trait object across the FFI boundary (see `lease.rs`).
//! - Pub/sub streaming (`publish`, `subscribe`, `watch_peers`)
//!   over NATS Core, bridged to the host via the streaming FFI
//!   shim (see `pubsub.rs`).
//!
//! # Why a bundled tokio runtime
//!
//! The `cluster_backend` FFI is sync; async-nats is async. Each
//! trait method block_on's its async closure on the bundled
//! runtime. Same pattern as cache.redis + secret.vault +
//! policy.opa.

mod client;
mod config;
mod error;
mod lease;
mod peer;
mod pubsub;
mod state;

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use tokio::runtime::Runtime;
use tokio::sync::OnceCell;

use mcpg_cluster_api::{
    BoxActiveLease, BoxPeerEventStream, BoxPublishedMessageStream, ClusterBackend, ClusterError,
    ClusterNodeInfo, ClusterPeer, KeyValueStore, Lease, PeerHealth, PubSub, Watch,
};
use mcpg_plugin_protocol::async_trait;
use mcpg_plugin_protocol::{PluginClass, PluginManifest};
use mcpg_plugin_sdk::declare_plugin;
use mcpg_plugin_sdk::ffi::{SyncClusterBackend, WatchHandleBox};

use crate::state::{NatsKv, NatsLock, NatsTopicBus, NatsWatch};

pub use lease::NatsJetStreamLeaseHandle;

pub use config::{
    AuthConfig, ClusterNatsConfig, ConnectionConfig, JetStreamConfig, LeaseConfig, NodeConfig,
    TlsConfig,
};
pub use error::ConfigError;

const PLUGIN_ID: &str = "dev.mcpg.cluster.nats";

pub struct NatsJetStreamCoordinator {
    inner: Arc<NatsJetStreamInner>,
}

struct NatsJetStreamInner {
    manifest: PluginManifest,
    config: ClusterNatsConfig,
    /// Broadcast channel `watch_peers` taps into. Created at
    /// construction so a `watch_peers` subscription can be opened
    /// before the broker is up; the peer workers (built lazily with
    /// the connection) publish into it. Capacity sized for typical
    /// peer churn — small clusters generate <1 event/s, 64 absorbs
    /// reasonable bursts without losing events.
    peer_events: pubsub::PeerEventSender,
    /// Plugin start timestamp (RFC3339).
    started_at_rfc3339: String,
    /// NATS connection + peer workers + primitive bundle, established
    /// LAZILY on first real use. `make` does NOT connect — a
    /// broker-down-at-boot is tolerated and the empty-config manifest
    /// probe builds a non-connecting instance. The first KV / pub-sub
    /// / lease / peer call funnels through `get_or_init_connected`,
    /// which connects, bootstraps the JS KV buckets, spawns the peer
    /// heartbeat workers, and builds the primitive bundle exactly
    /// once. Accessors return `None` until this cell is populated,
    /// matching the `ClusterBackend` contract.
    connected: OnceCell<Connected>,
    /// Bundled tokio runtime — held last in declaration order so
    /// it drops AFTER the `Connected` bundle (whose peer workers
    /// abort their tasks during drop). Avoids the runtime tearing
    /// down while a task is still on it.
    runtime: Runtime,
}

/// Live connection-dependent state, built once on first use by
/// `get_or_init_connected`.
struct Connected {
    /// NATS connection + bootstrapped KV buckets. Peer workers
    /// borrow `.nats`; lease + fencing logic borrows `.leases` /
    /// `.fencing`.
    client: client::NatsClientHandle,
    /// Heartbeat publisher + subscriber tasks + peer cache.
    /// Drop aborts both tasks; the `peer::PeerWorkers::Drop` impl
    /// ties task lifetime to the plugin handle lifetime.
    peers: peer::PeerWorkers,
    /// Primitive bundle the gateway's capabilities inherit when
    /// binding `cluster: { kind: nats }`. Built from the connected
    /// NATS client + the dedicated `state_bucket` JS KV store. All
    /// primitives share the same connection (cheap `Client` clone)
    /// so the cluster plugin presents one connection to NATS for the
    /// whole gateway.
    primitives: NatsPrimitives,
}

/// Primitive impls sharing the cluster plugin's NATS connection.
/// Built once when the connection comes up; returned `Arc`-cloned
/// from each accessor call.
struct NatsPrimitives {
    kv: Arc<NatsKv>,
    lease: Arc<NatsLock>,
    pub_sub: Arc<NatsTopicBus>,
    /// JS KV native change-notification stream over the same
    /// `state_bucket` `kv` writes into. `watch_all` per subscriber;
    /// prefix filter applied client-side.
    watch: Arc<NatsWatch>,
}

impl NatsJetStreamCoordinator {
    pub fn from_config_json(config_json: &str) -> Self {
        // Load-time manifest derivation builds + drops an instance only to
        // read its plugin-wide manifest. It has no real connection config, so
        // the host passes the manifest-probe sentinel (`{}`). Substitute a
        // placeholder config (lazy connect, no eager network I/O) so
        // construction succeeds for that probe; a REAL config still flows
        // through parse + validate below, so a genuinely misconfigured
        // coordinator still refuses to load.
        if mcpg_plugin_protocol::is_manifest_probe_config(config_json) {
            let cfg = ClusterNatsConfig::parse(
                "{\"servers\":[\"nats://127.0.0.1:4222\"],\"node\":{\"id\":\"manifest-probe\"}}",
            )
            .expect("manifest-probe placeholder nats config is valid");
            return Self::from_validated_config(cfg);
        }
        let config = ClusterNatsConfig::parse(config_json).unwrap_or_else(|err| {
            tracing::error!(
                plugin_id = PLUGIN_ID,
                error = %err,
                "nats cluster: config parse failed; refusing to register"
            );
            panic!("nats cluster config parse failed: {err}")
        });
        Self::from_validated_config(config)
    }

    fn from_validated_config(config: ClusterNatsConfig) -> Self {
        // Multi-thread runtime: one worker for the heartbeat
        // publisher, one for the subscriber + sweeper, with
        // headroom for the lease-renewal and pub/sub worker
        // tasks.
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .thread_name("mcpg-cluster-nats")
            .enable_all()
            .build()
            .unwrap_or_else(|err| {
                tracing::error!(
                    plugin_id = PLUGIN_ID,
                    error = %err,
                    "nats cluster: tokio runtime init failed; refusing to register"
                );
                panic!("nats cluster tokio runtime init failed: {err}")
            });

        let started_at_rfc3339 = now_rfc3339();
        let peer_events = pubsub::new_peer_event_channel(64);

        let inner = Arc::new(NatsJetStreamInner {
            manifest: PluginManifest {
                id: PLUGIN_ID.into(),
                version: env!("CARGO_PKG_VERSION").into(),
                name: "NATS JetStream Cluster Coordinator".into(),
                plugin_class: PluginClass::Cluster,
                protocol_version: "1.0".into(),
                license: None,
                required_capabilities: Vec::new(),
                tags: Vec::new(),
                // Slot roles (cache/kv/bus), not primitive accessors.
                // NATS JetStream backs `kv` (JS KV buckets) and `bus`
                // (subjects / JetStream consumers). No cache-eviction
                // role today.
                provides: vec!["kv".into(), "bus".into()],
                provides_schemes: Vec::new(),
                module_path_prefix: ::std::module_path!()
                    .split("::")
                    .next()
                    .unwrap_or("")
                    .to_owned(),
                backend_profile: None,
            },
            config,
            peer_events,
            started_at_rfc3339,
            connected: OnceCell::new(),
            runtime,
        });

        // Best-effort eager connect. If NATS is up at boot the `connected`
        // cell populates and the gateway's capabilities get real
        // `key_value_store()` / `lease()` / `pub_sub()` accessors + live peer
        // workers. If NATS is down the init returns `BackendUnavailable`, the
        // cell stays empty, and the next real call (or accessor probe)
        // retries the connection — at which point everything populates.
        // Accessors return `None` until then, matching the contract on
        // `ClusterBackend`.
        {
            let init_inner = Arc::clone(&inner);
            inner.runtime.block_on(async move {
                if let Err(err) = get_or_init_connected(&init_inner).await {
                    tracing::warn!(
                        plugin_id = PLUGIN_ID,
                        error = %err,
                        "nats cluster: connection unavailable at boot — primitive \
                         accessors will return None and peer workers will start on \
                         first successful op"
                    );
                }
            });
        }

        Self { inner }
    }

    /// Block on the lazy connect, then resolve the live `Connected`
    /// bundle. Returns `BackendUnavailable` when NATS is unreachable.
    fn require_connected(&self) -> Result<&Connected, ClusterError> {
        self.inner
            .runtime
            .block_on(async { get_or_init_connected(&self.inner).await })?;
        self.inner
            .connected
            .get()
            .ok_or_else(|| ClusterError::BackendUnavailable {
                reason: "nats cluster: connection unavailable".into(),
            })
    }
}

/// Lazily connect to NATS, bootstrap the JS KV buckets, spawn the peer
/// heartbeat workers, and build the primitive bundle — exactly once.
/// Subsequent calls hit the `OnceCell` fast-path with no extra work.
/// Returns `BackendUnavailable` when NATS is unreachable so the caller
/// can decide whether to retry.
async fn get_or_init_connected(
    inner: &Arc<NatsJetStreamInner>,
) -> Result<&Connected, ClusterError> {
    inner
        .connected
        .get_or_try_init(|| async {
            let client = client::NatsClientHandle::connect(&inner.config).await?;

            let peers = peer::PeerWorkers::start(
                inner.runtime.handle(),
                client.nats.clone(),
                &inner.config,
                inner.started_at_rfc3339.clone(),
                Arc::clone(&inner.peer_events),
            );

            // Build the primitive bundle off the same connection the
            // coordinator holds. The `KeyValueStore` primitive points at
            // `state_bucket`; `Lease` reuses the KV-via-`with_store`
            // constructor on the dedicated leases bucket; `PubSub` shares the
            // connected `Client`. `Watch` runs over the same `state_bucket`
            // so put/delete events from the coordinator's `KeyValueStore`
            // accessor surface to subscribers.
            let primitives = NatsPrimitives {
                kv: Arc::new(NatsKv::with_store(client.state.clone())),
                lease: Arc::new(NatsLock::with_store(client.leases.clone())),
                pub_sub: Arc::new(NatsTopicBus::with_client(client.nats.clone())),
                watch: Arc::new(NatsWatch::with_store(client.state.clone())),
            };

            Ok::<Connected, ClusterError>(Connected {
                client,
                peers,
                primitives,
            })
        })
        .await
}

/// In-process async impl. Mirrors every [`SyncClusterBackend`]
/// method via the same internal modules' async cores, without the
/// FFI shim's `block_on` indirection.
///
/// Used by:
///
/// - The shared coordinator equivalence test suite
///   (`mcpg-cluster-equivalence-tests`).
/// - Future static-firstparty deploys that link the plugin
///   directly into the gateway and need a real Rust async surface
///   (avoiding the per-call FFI round-trip).
///
/// Background tasks (lease renewal, heartbeat publisher /
/// subscriber, peer-cache sweeper) all live on the plugin's
/// bundled `runtime: Runtime`, regardless of which surface the
/// caller used. That keeps task lifetime tied to the plugin
/// handle rather than the caller's runtime — matches the FFI
/// path's behaviour exactly.
#[async_trait]
impl ClusterBackend for NatsJetStreamCoordinator {
    // `cluster_provides()` uses the default impl: it derives the role
    // set from `manifest().provides` (= kv, bus).

    fn manifest(&self) -> &PluginManifest {
        &self.inner.manifest
    }

    fn key_value_store(&self) -> Option<Arc<dyn KeyValueStore>> {
        self.inner
            .connected
            .get()
            .map(|c| Arc::clone(&c.primitives.kv) as Arc<dyn KeyValueStore>)
    }

    fn pub_sub(&self) -> Option<Arc<dyn PubSub>> {
        self.inner
            .connected
            .get()
            .map(|c| Arc::clone(&c.primitives.pub_sub) as Arc<dyn PubSub>)
    }

    fn lease(&self) -> Option<Arc<dyn Lease>> {
        self.inner
            .connected
            .get()
            .map(|c| Arc::clone(&c.primitives.lease) as Arc<dyn Lease>)
    }

    fn watch(&self) -> Option<Arc<dyn Watch>> {
        self.inner
            .connected
            .get()
            .map(|c| Arc::clone(&c.primitives.watch) as Arc<dyn Watch>)
    }

    async fn node_info(&self) -> ClusterNodeInfo {
        let _ = PeerHealth::Healthy; // keep the import live; downstream uses it via list_peers
        // Connect lazily; if the broker is down, report no held roles
        // rather than failing the (infallible) node_info contract.
        let roles = match get_or_init_connected(&self.inner).await {
            Ok(c) => {
                lease::enumerate_held_roles(&c.client.leases, &self.inner.config.node.id).await
            }
            Err(_) => Vec::new(),
        };
        ClusterNodeInfo {
            node_id: self.inner.config.node.id.clone(),
            address: self.inner.config.node.address.clone().unwrap_or_default(),
            version: env!("CARGO_PKG_VERSION").into(),
            started_at: self.inner.started_at_rfc3339.clone(),
            roles,
        }
    }

    async fn list_peers(&self) -> Vec<ClusterPeer> {
        // Connect lazily so the peer workers start; an unreachable broker
        // yields an empty snapshot rather than failing the contract.
        match get_or_init_connected(&self.inner).await {
            Ok(c) => c.peers.snapshot_async().await,
            Err(_) => Vec::new(),
        }
    }

    async fn watch_peers(&self) -> BoxPeerEventStream {
        // Subscribe to the shared event channel first so events are not
        // missed, then kick the lazy connect to start the peer workers
        // that publish into it.
        let rx = self.inner.peer_events.subscribe();
        let _ = get_or_init_connected(&self.inner).await;
        pubsub::watch_peers_stream(rx)
    }

    async fn publish(
        &self,
        topic: &str,
        routing_key: Option<&str>,
        payload: Bytes,
    ) -> Result<(), ClusterError> {
        let c = get_or_init_connected(&self.inner).await?;
        pubsub::publish_async(
            &c.client.nats,
            &self.inner.config.node.id,
            topic,
            routing_key,
            payload,
        )
        .await
    }

    async fn subscribe(
        &self,
        topic: &str,
        group: Option<&str>,
        routing_key: Option<&str>,
    ) -> Result<BoxPublishedMessageStream, ClusterError> {
        let c = get_or_init_connected(&self.inner).await?;
        pubsub::subscribe_async(
            c.client.nats.clone(),
            topic.to_owned(),
            group.map(str::to_owned),
            routing_key.map(str::to_owned),
        )
        .await
    }

    async fn acquire_leadership(
        &self,
        role: &str,
        lease_ttl: Duration,
    ) -> Result<BoxActiveLease, ClusterError> {
        validate_lease_key("role", role)?;
        let c = get_or_init_connected(&self.inner).await?;
        let handle = lease::acquire_async(
            self.inner.runtime.handle().clone(),
            lease::AcquireParams {
                leases: c.client.leases.clone(),
                fencing: c.client.fencing.clone(),
                key: format!("{}.{role}", lease::LEADERSHIP_PREFIX),
                node_id: self.inner.config.node.id.clone(),
                ttl_ms: normalise_ttl_ms(duration_to_ttl_ms(lease_ttl), &self.inner.config),
                wait: true,
                renew_before_expiry_percent: self.inner.config.lease.renew_before_expiry_percent,
            },
        )
        .await?;
        Ok(Box::new(handle))
    }

    async fn acquire_lock(
        &self,
        key: &str,
        lease_ttl: Duration,
    ) -> Result<BoxActiveLease, ClusterError> {
        validate_lease_key("key", key)?;
        let c = get_or_init_connected(&self.inner).await?;
        let handle = lease::acquire_async(
            self.inner.runtime.handle().clone(),
            lease::AcquireParams {
                leases: c.client.leases.clone(),
                fencing: c.client.fencing.clone(),
                key: format!("{}.{key}", lease::LOCK_PREFIX),
                node_id: self.inner.config.node.id.clone(),
                ttl_ms: normalise_ttl_ms(duration_to_ttl_ms(lease_ttl), &self.inner.config),
                wait: true,
                renew_before_expiry_percent: self.inner.config.lease.renew_before_expiry_percent,
            },
        )
        .await?;
        Ok(Box::new(handle))
    }

    async fn try_acquire_leadership(
        &self,
        role: &str,
        lease_ttl: Duration,
    ) -> Result<Option<BoxActiveLease>, ClusterError> {
        validate_lease_key("role", role)?;
        let c = get_or_init_connected(&self.inner).await?;
        let h = lease::try_acquire_async(
            self.inner.runtime.handle().clone(),
            lease::AcquireParams {
                leases: c.client.leases.clone(),
                fencing: c.client.fencing.clone(),
                key: format!("{}.{role}", lease::LEADERSHIP_PREFIX),
                node_id: self.inner.config.node.id.clone(),
                ttl_ms: normalise_ttl_ms(duration_to_ttl_ms(lease_ttl), &self.inner.config),
                wait: false,
                renew_before_expiry_percent: self.inner.config.lease.renew_before_expiry_percent,
            },
        )
        .await?;
        Ok(h.map(|x| Box::new(x) as BoxActiveLease))
    }

    async fn try_acquire_lock(
        &self,
        key: &str,
        lease_ttl: Duration,
    ) -> Result<Option<BoxActiveLease>, ClusterError> {
        validate_lease_key("key", key)?;
        let c = get_or_init_connected(&self.inner).await?;
        let h = lease::try_acquire_async(
            self.inner.runtime.handle().clone(),
            lease::AcquireParams {
                leases: c.client.leases.clone(),
                fencing: c.client.fencing.clone(),
                key: format!("{}.{key}", lease::LOCK_PREFIX),
                node_id: self.inner.config.node.id.clone(),
                ttl_ms: normalise_ttl_ms(duration_to_ttl_ms(lease_ttl), &self.inner.config),
                wait: false,
                renew_before_expiry_percent: self.inner.config.lease.renew_before_expiry_percent,
            },
        )
        .await?;
        Ok(h.map(|x| Box::new(x) as BoxActiveLease))
    }

    async fn shutdown(&self) {
        // Proactively abort the heartbeat publisher + subscriber tasks
        // now, rather than waiting for `PeerWorkers::Drop` (which fires
        // only when the last `Arc<Inner>` ref is released — possibly
        // delayed by an outstanding lease handle). No-op if the broker
        // was never reached (no peer workers spawned).
        if let Some(c) = self.inner.connected.get() {
            c.peers.abort();
        }
        tracing::info!(
            plugin_id = PLUGIN_ID,
            "nats cluster: shutdown — peer workers aborted"
        );
    }
}

impl SyncClusterBackend for NatsJetStreamCoordinator {
    fn manifest(&self) -> &PluginManifest {
        &self.inner.manifest
    }

    /// Abort the peer-worker background tasks within the host drain
    /// window instead of relying on `Drop`. No-op if the broker was
    /// never reached (no peer workers spawned).
    fn shutdown(&self) {
        if let Some(c) = self.inner.connected.get() {
            c.peers.abort();
        }
        tracing::info!(
            plugin_id = PLUGIN_ID,
            "nats cluster: shutdown — peer workers aborted"
        );
    }

    fn node_info(&self) -> ClusterNodeInfo {
        // Enumerate the roles this node currently holds by
        // scanning the leases bucket. The cost is a single KV
        // bucket-scan per call; admin surfaces poll
        // infrequently so this is cheap enough to do live. A
        // broker-down state yields no held roles rather than failing.
        let self_id = self.inner.config.node.id.clone();
        let roles = match self.require_connected() {
            Ok(c) => {
                let leases = c.client.leases.clone();
                self.inner
                    .runtime
                    .block_on(async move { lease::enumerate_held_roles(&leases, &self_id).await })
            }
            Err(_) => Vec::new(),
        };

        ClusterNodeInfo {
            node_id: self.inner.config.node.id.clone(),
            address: self.inner.config.node.address.clone().unwrap_or_default(),
            version: env!("CARGO_PKG_VERSION").into(),
            started_at: self.inner.started_at_rfc3339.clone(),
            roles,
        }
    }

    fn list_peers(&self) -> Vec<ClusterPeer> {
        // Snapshot the peer cache populated by the heartbeat
        // subscriber. Excludes the local node; Healthy/Degraded/
        // Unreachable classification is updated by the cache
        // sweeper on every `heartbeat_interval` tick. Returns empty
        // until the lazy connect brings the peer workers up.
        let _ = PeerHealth::Healthy; // used downstream
        match self.require_connected() {
            Ok(c) => c.peers.snapshot(self.inner.runtime.handle()),
            Err(_) => Vec::new(),
        }
    }

    fn publish(
        &self,
        topic: &str,
        routing_key: Option<&str>,
        payload: Vec<u8>,
    ) -> Result<(), ClusterError> {
        let c = self.require_connected()?;
        pubsub::publish_sync(
            self.inner.runtime.handle(),
            &c.client.nats,
            &self.inner.config.node.id,
            topic,
            routing_key,
            payload,
        )
    }

    fn subscribe(
        &self,
        topic: &str,
        group: Option<&str>,
        routing_key: Option<&str>,
        emit_event: Box<dyn Fn(&str) + Send + Sync + 'static>,
    ) -> Result<WatchHandleBox, ClusterError> {
        let c = self.require_connected()?;
        pubsub::subscribe(
            self.inner.runtime.handle(),
            c.client.nats.clone(),
            topic.to_owned(),
            group.map(str::to_owned),
            routing_key.map(str::to_owned),
            emit_event,
        )
    }

    fn watch_peers(
        &self,
        emit_event: Box<dyn Fn(&str) + Send + Sync + 'static>,
    ) -> Result<WatchHandleBox, ClusterError> {
        // Subscribe to the shared event channel first, then kick the lazy
        // connect so the peer workers that publish into it come up.
        let rx = self.inner.peer_events.subscribe();
        let _ = self.require_connected();
        pubsub::watch_peers(self.inner.runtime.handle(), rx, emit_event)
    }

    fn cancel_stream(&self, h: WatchHandleBox) {
        // SAFETY: the host vtable contract — handle was produced
        // by `subscribe` / `watch_peers`, hasn't been cancelled
        // yet. The `drop_stream` helper reclaims the leaked
        // `Box<StreamState>` and the resulting drop fires the
        // forwarder task's AbortHandle.
        unsafe { pubsub::drop_stream(h) }
    }

    /// Acquire leadership for a role. Waits until available — the impl
    /// polls the KV slot every 500ms until it's free, bounded by ~10×TTL
    /// so a stuck cluster surfaces a Timeout instead of hanging the
    /// caller forever.
    fn acquire_leadership(
        &self,
        role: &str,
        ttl_ms: u64,
    ) -> Result<(WatchHandleBox, u64, String), ClusterError> {
        validate_lease_key("role", role)?;
        let c = self.require_connected()?;
        lease::acquire(
            self.inner.runtime.handle(),
            lease::AcquireParams {
                leases: c.client.leases.clone(),
                fencing: c.client.fencing.clone(),
                key: format!("{}.{role}", lease::LEADERSHIP_PREFIX),
                node_id: self.inner.config.node.id.clone(),
                ttl_ms: normalise_ttl_ms(ttl_ms, &self.inner.config),
                wait: true,
                renew_before_expiry_percent: self.inner.config.lease.renew_before_expiry_percent,
            },
        )
    }

    /// Acquire a distributed fenced lock. Per ClusterBackend
    /// trait contract this BLOCKS until the lock becomes available
    /// (bounded by `wait: true` in the impl below); callers that
    /// want immediate-decline-on-contention use [`try_acquire_lock`].
    fn acquire_lock(
        &self,
        key: &str,
        ttl_ms: u64,
    ) -> Result<(WatchHandleBox, u64, String), ClusterError> {
        validate_lease_key("key", key)?;
        let c = self.require_connected()?;
        lease::acquire(
            self.inner.runtime.handle(),
            lease::AcquireParams {
                leases: c.client.leases.clone(),
                fencing: c.client.fencing.clone(),
                key: format!("{}.{key}", lease::LOCK_PREFIX),
                node_id: self.inner.config.node.id.clone(),
                ttl_ms: normalise_ttl_ms(ttl_ms, &self.inner.config),
                wait: true,
                renew_before_expiry_percent: self.inner.config.lease.renew_before_expiry_percent,
            },
        )
    }

    /// Non-blocking leadership acquire. Single-shot JetStream KV
    /// CAS. Returns `Ok(None)` on contention.
    fn try_acquire_leadership(
        &self,
        role: &str,
        ttl_ms: u64,
    ) -> Result<Option<(WatchHandleBox, u64, String)>, ClusterError> {
        validate_lease_key("role", role)?;
        let c = self.require_connected()?;
        lease::try_acquire(
            self.inner.runtime.handle(),
            lease::AcquireParams {
                leases: c.client.leases.clone(),
                fencing: c.client.fencing.clone(),
                key: format!("{}.{role}", lease::LEADERSHIP_PREFIX),
                node_id: self.inner.config.node.id.clone(),
                ttl_ms: normalise_ttl_ms(ttl_ms, &self.inner.config),
                wait: false,
                renew_before_expiry_percent: self.inner.config.lease.renew_before_expiry_percent,
            },
        )
    }

    /// Non-blocking lock acquire. Single-shot JetStream KV CAS.
    /// Returns `Ok(None)` on contention.
    fn try_acquire_lock(
        &self,
        key: &str,
        ttl_ms: u64,
    ) -> Result<Option<(WatchHandleBox, u64, String)>, ClusterError> {
        validate_lease_key("key", key)?;
        let c = self.require_connected()?;
        lease::try_acquire(
            self.inner.runtime.handle(),
            lease::AcquireParams {
                leases: c.client.leases.clone(),
                fencing: c.client.fencing.clone(),
                key: format!("{}.{key}", lease::LOCK_PREFIX),
                node_id: self.inner.config.node.id.clone(),
                ttl_ms: normalise_ttl_ms(ttl_ms, &self.inner.config),
                wait: false,
                renew_before_expiry_percent: self.inner.config.lease.renew_before_expiry_percent,
            },
        )
    }

    fn lease_renew(&self, lease_handle: WatchHandleBox) -> Result<String, ClusterError> {
        // SAFETY: host vtable contract — handle was produced by a
        // prior `acquire_*`, hasn't been dropped, lives for the
        // duration of this call.
        let state = unsafe { lease::borrow_state(&lease_handle) };
        let state = state.ok_or(ClusterError::LeaseExpired)?;
        lease::renew(self.inner.runtime.handle(), state)
    }

    fn lease_release(&self, lease_handle: WatchHandleBox) -> Result<(), ClusterError> {
        let state = unsafe { lease::borrow_state(&lease_handle) };
        let state = match state {
            Some(s) => s,
            None => return Ok(()),
        };
        lease::release(self.inner.runtime.handle(), state)
    }

    fn lease_drop(&self, lease_handle: WatchHandleBox) {
        // SAFETY: host vtable contract — exactly one `lease_drop`
        // per acquire, and the pointer is still valid.
        unsafe { lease::drop_state(lease_handle) }
    }

    // KV primitive over FFI — block on the plugin's own runtime, routing
    // each method through the JetStream `KeyValueStore` impl that
    // `key_value_store()` exposes.
    fn kv_get(&self, key: &str) -> Result<Option<mcpg_cluster_api::Entry>, ClusterError> {
        let kv = Arc::clone(&self.require_connected()?.primitives.kv);
        self.inner
            .runtime
            .block_on(async move { kv.get(key).await })
    }

    fn kv_put(&self, key: &str, value: Vec<u8>, ttl_ms: Option<u64>) -> Result<(), ClusterError> {
        let kv = Arc::clone(&self.require_connected()?.primitives.kv);
        self.inner
            .runtime
            .block_on(async move { kv.put(key, Bytes::from(value), ttl_from_ms(ttl_ms)).await })
    }

    fn kv_put_if_absent(
        &self,
        key: &str,
        value: Vec<u8>,
        ttl_ms: Option<u64>,
    ) -> Result<bool, ClusterError> {
        let kv = Arc::clone(&self.require_connected()?.primitives.kv);
        self.inner.runtime.block_on(async move {
            kv.put_if_absent(key, Bytes::from(value), ttl_from_ms(ttl_ms))
                .await
        })
    }

    fn kv_delete(&self, key: &str) -> Result<bool, ClusterError> {
        let kv = Arc::clone(&self.require_connected()?.primitives.kv);
        self.inner
            .runtime
            .block_on(async move { kv.delete(key).await })
    }

    fn kv_list_prefix(
        &self,
        prefix: &str,
        limit: usize,
    ) -> Result<Vec<(String, mcpg_cluster_api::Entry)>, ClusterError> {
        let kv = Arc::clone(&self.require_connected()?.primitives.kv);
        self.inner
            .runtime
            .block_on(async move { kv.list_prefix(prefix, limit).await })
    }

    fn kv_expire(&self, key: &str, ttl_ms: Option<u64>) -> Result<bool, ClusterError> {
        let kv = Arc::clone(&self.require_connected()?.primitives.kv);
        self.inner
            .runtime
            .block_on(async move { kv.expire(key, ttl_from_ms(ttl_ms)).await })
    }
}

/// Whole-millisecond TTL → `Duration` (None == no TTL).
fn ttl_from_ms(ttl_ms: Option<u64>) -> Option<Duration> {
    ttl_ms.map(Duration::from_millis)
}

declare_plugin! {
    plugin_id: PLUGIN_ID,
    plugin_version: env!("CARGO_PKG_VERSION"),
    descriptor_yaml: include_str!("../plugin.yaml"),
    capabilities: &[mcpg_plugin_protocol::capability::Capability::NetworkOutbound],
    entities: [
        cluster_backend as cluster {
            inner_name: "",
            plugin_type: NatsJetStreamCoordinator,
            factory: |cfg: &str, _host: ::mcpg_plugin_sdk::HostHandle| NatsJetStreamCoordinator::from_config_json(cfg),
        }
    ],
}

/// Reject empty / whitespace-only lease names + names that JS KV
/// rejects at the wire level, before we prepend the namespace
/// prefix. JS KV's only hard rules: non-empty, must not start or
/// end with `.`, must not contain the NATS subject wildcards
/// (`*`, `>`). Dotted keys like `orders.write` are fine.
fn validate_lease_key(label: &str, key: &str) -> Result<(), ClusterError> {
    let trimmed = key.trim();
    if trimmed.is_empty() {
        return Err(ClusterError::InvalidReference {
            message: format!("lease {label} must not be empty"),
        });
    }
    if key.starts_with('.') || key.ends_with('.') {
        return Err(ClusterError::InvalidReference {
            message: format!(
                "lease {label} `{key}` must not start or end with `.` (JS KV constraint)"
            ),
        });
    }
    if key.contains('*') || key.contains('>') {
        return Err(ClusterError::InvalidReference {
            message: format!(
                "lease {label} `{key}` must not contain NATS subject wildcards (`*` / `>`)"
            ),
        });
    }
    Ok(())
}

/// Coerce the trait's `ttl_ms == 0` sentinel into the operator-
/// configured default lease TTL. Mirrors the cache.redis plugin's
/// "ttl=0 → expire on next tick" semantic but for leases the
/// trait doesn't define a zero-meaning, so we substitute the
/// configured `lease.default_ttl_sec`.
fn normalise_ttl_ms(ttl_ms: u64, cfg: &ClusterNatsConfig) -> u64 {
    if ttl_ms == 0 {
        cfg.lease.default_ttl_sec.saturating_mul(1_000)
    } else {
        ttl_ms
    }
}

/// Convert a `Duration` from the in-process async trait surface
/// into the `u64 ms` shape the lease module's internal helpers
/// already expect. Saturates at `u64::MAX` so a pathologically
/// long TTL (years) doesn't wrap.
fn duration_to_ttl_ms(d: Duration) -> u64 {
    u64::try_from(d.as_millis()).unwrap_or(u64::MAX)
}

/// Bare RFC 3339 timestamp helper. We reach for chrono nowhere
/// else in this crate; rolling a tiny formatter avoids the dep.
/// Same shape as the helper in `mcpg-plugin-policy-opa`.
fn now_rfc3339() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;
    let (year, month, day, hour, min, sec) = epoch_to_ymdhms(secs);
    format!("{year:04}-{month:02}-{day:02}T{hour:02}:{min:02}:{sec:02}Z")
}

pub(crate) fn epoch_to_ymdhms(secs: i64) -> (i32, u32, u32, u32, u32, u32) {
    let days_since_epoch = secs.div_euclid(86_400);
    let secs_today = secs.rem_euclid(86_400) as u32;
    let hour = secs_today / 3600;
    let min = (secs_today % 3600) / 60;
    let sec = secs_today % 60;
    let z = days_since_epoch + 719_468;
    let era = z.div_euclid(146_097);
    let doe = (z - era * 146_097) as u32;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let y = yoe as i32 + era as i32 * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = if m <= 2 { y + 1 } else { y };
    (year, m, d, hour, min, sec)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `from_config_json` connects LAZILY (the connection is established on
    /// first real primitive use), so construction succeeds without a live
    /// broker — the unit tests here exercise that, plus the manifest-probe
    /// path, config error rejection, and descriptor / manifest shape.
    /// End-to-end primitive round-trips against a real NATS live in the
    /// integration suite (`tests/integration.rs`, `--features
    /// integration-tests`).

    #[test]
    #[should_panic(expected = "nats cluster config parse failed")]
    fn factory_panics_on_unparseable_config() {
        let _ = NatsJetStreamCoordinator::from_config_json("not-json");
    }

    #[test]
    fn factory_builds_without_a_live_broker() {
        // A real (valid) config pointing at an unreachable broker must
        // still construct: connect is deferred to first use. Accessors
        // report `None` until the connection comes up.
        let coordinator = NatsJetStreamCoordinator::from_config_json(
            r#"{"servers":["nats://127.0.0.1:1"],"node":{"id":"g1"}}"#,
        );
        let manifest = ClusterBackend::manifest(&coordinator);
        assert_eq!(manifest.id, PLUGIN_ID);
        assert!(ClusterBackend::key_value_store(&coordinator).is_none());
        assert!(ClusterBackend::pub_sub(&coordinator).is_none());
        assert!(ClusterBackend::lease(&coordinator).is_none());
    }

    #[test]
    fn manifest_probe_builds_non_connecting_instance() {
        // The host's load-time manifest derivation passes `{}`; that must
        // build a placeholder without connecting and expose the real
        // manifest (provides = kv, bus).
        let coordinator = NatsJetStreamCoordinator::from_config_json("{}");
        let manifest = ClusterBackend::manifest(&coordinator);
        assert_eq!(manifest.id, PLUGIN_ID);
        assert!(manifest.provides.contains(&"kv".to_string()));
        assert!(manifest.provides.contains(&"bus".to_string()));
        assert!(matches!(manifest.plugin_class, PluginClass::Cluster));
    }

    #[test]
    #[should_panic(expected = "nats cluster config parse failed")]
    fn non_empty_but_invalid_real_config_still_rejected() {
        // The probe tolerance only applies to the empty `{}` sentinel — a
        // non-empty-but-invalid real config (no servers) must still panic.
        let _ = NatsJetStreamCoordinator::from_config_json(r#"{"servers":[]}"#);
    }

    #[test]
    fn kv_get_surfaces_unreachable_as_backend_unavailable() {
        let coordinator = NatsJetStreamCoordinator::from_config_json(
            r#"{"servers":["nats://127.0.0.1:1"],"node":{"id":"g1"}}"#,
        );
        let err = SyncClusterBackend::kv_get(&coordinator, "k")
            .expect_err("expected BackendUnavailable against an unreachable broker");
        assert!(
            matches!(err, ClusterError::BackendUnavailable { .. }),
            "expected BackendUnavailable, got {err:?}"
        );
    }

    #[test]
    fn descriptor_yaml_is_well_formed() {
        assert!(DESCRIPTOR_YAML.contains(&format!("id: {PLUGIN_ID}")));
        assert!(DESCRIPTOR_YAML.contains("class: cluster"));
        assert!(DESCRIPTOR_YAML.contains("runtime: native-cdylib-v1"));
        assert!(DESCRIPTOR_YAML.contains("network_outbound"));
    }

    #[test]
    fn epoch_to_rfc3339_renders_known_instant() {
        // Unix epoch itself = 1970-01-01T00:00:00Z.
        assert_eq!(epoch_to_ymdhms(0), (1970, 1, 1, 0, 0, 0));
        // 2000-01-01T00:00:00Z = 946684800 (well-known reference).
        assert_eq!(epoch_to_ymdhms(946_684_800), (2000, 1, 1, 0, 0, 0));
        // 2024-02-29T12:34:56Z exercises a leap-day branch.
        // 2024-02-29 = 19782 days post-epoch; 12:34:56 = 45296s.
        let secs = 19782 * 86400 + 45296;
        assert_eq!(epoch_to_ymdhms(secs), (2024, 2, 29, 12, 34, 56));
    }
}
