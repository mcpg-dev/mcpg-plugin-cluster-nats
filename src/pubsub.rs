//! Pub/sub + watch_peers via the streaming FFI surface.
//!
//! - `publish(topic, routing_key, payload)` — NATS core publish on
//!   `mcpg.notify.{topic}` (or `mcpg.notify.{topic}.{routing_key}`
//!   when set). Fire-and-forget — operators wanting
//!   delivery guarantees use the `binding.nats` plugin's
//!   request/reply path.
//! - `subscribe(topic, group, routing_key, emit_event)` —
//!   subscribes to the matching subject pattern. `group = Some(...)`
//!   uses NATS queue-group semantics so subscribers across replicas
//!   load-balance; `group = None` is broadcast (every subscriber
//!   gets every message).
//! - `watch_peers(emit_event)` — taps into the broadcast channel
//!   the heartbeat subscriber + sweeper publish on; emits
//!   `PeerEvent` JSON for Joined / Left / HealthChanged.
//!
//! Lifecycle: each subscribe / watch_peers spawns a forwarder
//! task on the plugin's runtime; the returned `WatchHandleBox`
//! wraps a `Box<StreamState>` whose `Drop` aborts the task AND
//! waits (bounded) for the forwarder future to be dropped, so an
//! in-flight `emit_event` cannot touch the host's freed StreamBridge
//!. `cancel_stream` reclaims the box via `Box::from_raw`.

use std::sync::Arc;

use async_nats::Client as NatsClient;
use bytes::Bytes;
use futures::StreamExt;
use mcpg_cluster_api::{
    BoxPeerEventStream, BoxPublishedMessageStream, ClusterError, PeerEvent, PublishedMessage,
};
use mcpg_plugin_sdk::ffi::WatchHandleBox;
use tokio::runtime::Handle;
use tokio::sync::broadcast;
use tokio::task::AbortHandle;

const NOTIFY_SUBJECT_PREFIX: &str = "mcpg.notify";

/// Reserved routing-key token used when the caller passes
/// `routing_key: None`. NATS subject semantics require a constant
/// arity for `*`-wildcard matching to work, so we always emit
/// 4-token subjects (`mcpg.notify.<topic>.<rk>`) and substitute
/// this sentinel in the no-routing-key case.
///
/// The token is rejected if an operator passes it explicitly via
/// `routing_key: Some("_default_")` — the bare reserved word is
/// out of bounds. See [`reject_reserved_routing_key`].
const DEFAULT_ROUTING_KEY: &str = "_default_";

/// Render the NATS publish subject. Always 4 tokens — the
/// no-routing-key case substitutes [`DEFAULT_ROUTING_KEY`] so
/// subscribers using the `*` wildcard pattern reliably match.
fn render_subject(topic: &str, routing_key: Option<&str>) -> String {
    let rk = routing_key
        .filter(|k| !k.is_empty())
        .unwrap_or(DEFAULT_ROUTING_KEY);
    format!("{NOTIFY_SUBJECT_PREFIX}.{topic}.{rk}")
}

/// Render the NATS subscribe subject pattern. With
/// `routing_key: Some(k)` we filter on the exact key; with
/// `None` we use the `*` single-token wildcard so we receive
/// messages from publishers using any routing key (including
/// the [`DEFAULT_ROUTING_KEY`] sentinel for no-key publishes).
fn render_subject_pattern(topic: &str, routing_key: Option<&str>) -> String {
    match routing_key {
        Some(k) if !k.is_empty() => format!("{NOTIFY_SUBJECT_PREFIX}.{topic}.{k}"),
        _ => format!("{NOTIFY_SUBJECT_PREFIX}.{topic}.*"),
    }
}

/// Reject operator-supplied routing keys that collide with the
/// reserved sentinel. Empty / `None` are not "supplied"; only
/// `Some(non_empty)` enters this check.
fn reject_reserved_routing_key(rk: Option<&str>) -> Result<(), ClusterError> {
    if matches!(rk, Some(k) if k == DEFAULT_ROUTING_KEY) {
        return Err(ClusterError::InvalidReference {
            message: format!(
                "routing_key `{DEFAULT_ROUTING_KEY}` is reserved by the nats-jetstream \
                 coordinator (used as the no-routing-key sentinel); pick a different value"
            ),
        });
    }
    Ok(())
}

/// Async core for [`publish_sync`]. Builds the wire subject,
/// stamps the `X-Mcpg-From-Node` header, and fires NATS core
/// publish. Used directly by the in-process async
/// `ClusterBackend` impl; [`publish_sync`] is a thin
/// `block_on` wrapper for the FFI vtable.
pub(crate) async fn publish_async(
    nats: &NatsClient,
    self_node_id: &str,
    topic: &str,
    routing_key: Option<&str>,
    payload: Bytes,
) -> Result<(), ClusterError> {
    if topic.trim().is_empty() {
        return Err(ClusterError::InvalidReference {
            message: "publish topic must not be empty".into(),
        });
    }
    reject_reserved_routing_key(routing_key)?;
    let subject = render_subject(topic, routing_key);
    // Encode the publisher's node id in the message header so
    // subscribers can populate `PublishedMessage.from_node`
    // without having to parse a wrapping envelope.
    let mut headers = async_nats::HeaderMap::new();
    headers.insert("X-Mcpg-From-Node", self_node_id);
    nats.publish_with_headers(subject.clone(), headers, payload)
        .await
        .map_err(|e| ClusterError::BackendUnavailable {
            reason: format!("nats publish {subject}: {e}"),
        })
}

/// Sync FFI wrapper around [`publish_async`].
pub(crate) fn publish_sync(
    runtime: &Handle,
    nats: &NatsClient,
    self_node_id: &str,
    topic: &str,
    routing_key: Option<&str>,
    payload: Vec<u8>,
) -> Result<(), ClusterError> {
    runtime.block_on(publish_async(
        nats,
        self_node_id,
        topic,
        routing_key,
        Bytes::from(payload),
    ))
}

/// Sends a quiescence signal when the forwarder task's future is
/// dropped — including on `abort()`, since aborting drops the future
/// and runs Drop on its locals. Held as a task-local so the signal
/// fires only after any in-flight, synchronous `emit_event` in the
/// current poll has returned (abort takes effect at the next `.await`).
struct ForwarderDoneSignal(std::sync::mpsc::Sender<()>);

impl Drop for ForwarderDoneSignal {
    fn drop(&mut self) {
        let _ = self.0.send(());
    }
}

/// Per-subscription state. Drop aborts the forwarder task **and waits
/// for it to quiesce** before returning.
pub(crate) struct StreamState {
    abort: AbortHandle,
    /// Receives one message once the forwarder future has been dropped.
    done_rx: std::sync::mpsc::Receiver<()>,
}

impl Drop for StreamState {
    fn drop(&mut self) {
        self.abort.abort();
        // the forwarder calls `emit_event` (a synchronous callback
        // into the host's StreamBridge) inside its loop; the host frees
        // that bridge as soon as our cancel slot returns. `abort()` only
        // signals at the next `.await`, so an in-flight `emit_event` would
        // otherwise complete AFTER the bridge is freed (use-after-free).
        // Block (bounded) until the forwarder future has been dropped — by
        // then any in-flight `emit_event` has returned and no further
        // callbacks can fire. Bounded so a wedged task can't hang teardown.
        match self.done_rx.recv_timeout(std::time::Duration::from_secs(5)) {
            Ok(()) => {}
            Err(_) => {
                tracing::warn!(
                    "nats cluster: forwarder did not quiesce within 5s of cancel; \
                     proceeding with teardown"
                );
            }
        }
    }
}

/// Spawn a NATS subscription forwarder. Each received message is
/// JSON-encoded as `PublishedMessage` and pushed through
/// `emit_event`.
pub(crate) fn subscribe(
    runtime: &Handle,
    nats: NatsClient,
    topic: String,
    group: Option<String>,
    routing_key: Option<String>,
    emit_event: Box<dyn Fn(&str) + Send + Sync + 'static>,
) -> Result<WatchHandleBox, ClusterError> {
    if topic.trim().is_empty() {
        return Err(ClusterError::InvalidReference {
            message: "subscribe topic must not be empty".into(),
        });
    }
    reject_reserved_routing_key(routing_key.as_deref())?;
    let pattern = render_subject_pattern(&topic, routing_key.as_deref());

    let (done_tx, done_rx) = std::sync::mpsc::channel::<()>();
    let join = runtime.spawn(async move {
        // Fires when this future is dropped (normal exit OR abort), so
        // StreamState::drop can wait for forwarder quiescence.
        let _done = ForwarderDoneSignal(done_tx);
        let mut sub = match group {
            Some(g) => match nats.queue_subscribe(pattern.clone(), g).await {
                Ok(s) => s,
                Err(e) => {
                    tracing::error!(
                        subject = %pattern,
                        error = %e,
                        "nats cluster: queue_subscribe failed; subscriber will not deliver"
                    );
                    return;
                }
            },
            None => match nats.subscribe(pattern.clone()).await {
                Ok(s) => s,
                Err(e) => {
                    tracing::error!(
                        subject = %pattern,
                        error = %e,
                        "nats cluster: subscribe failed; subscriber will not deliver"
                    );
                    return;
                }
            },
        };
        while let Some(msg) = sub.next().await {
            // Recover topic + routing_key from the subject. The
            // first segment after the prefix is the topic; the
            // remainder is the routing key.
            let routing = recover_routing_key(&topic, msg.subject.as_str());
            let from_node = msg
                .headers
                .as_ref()
                .and_then(|h| h.get("X-Mcpg-From-Node"))
                .map(|v| v.to_string())
                .unwrap_or_default();
            let pm = PublishedMessage {
                topic: topic.clone(),
                routing_key: routing,
                payload: msg.payload.clone(),
                from_node,
            };
            match serde_json::to_string(&pm) {
                Ok(s) => emit_event(&s),
                Err(e) => {
                    tracing::warn!(
                        subject = %msg.subject,
                        error = %e,
                        "nats cluster: PublishedMessage serialise failed; dropping"
                    );
                }
            }
        }
    });

    let state = Box::new(StreamState {
        abort: join.abort_handle(),
        done_rx,
    });
    Ok(WatchHandleBox(Box::into_raw(state) as *mut ()))
}

/// Async stream-returning subscribe used by the in-process
/// `ClusterBackend` impl. Wraps async-nats's native
/// `Subscriber` (which already implements `Stream<Item = Message>`)
/// and maps each NATS message to a `PublishedMessage`.
///
/// The returned stream owns the underlying NATS subscription;
/// dropping it closes the subscription with NATS automatically
/// (no extra task / AbortHandle needed). Same routing-key
/// recovery + `X-Mcpg-From-Node` header decode as the FFI path.
pub(crate) async fn subscribe_async(
    nats: NatsClient,
    topic: String,
    group: Option<String>,
    routing_key: Option<String>,
) -> Result<BoxPublishedMessageStream, ClusterError> {
    if topic.trim().is_empty() {
        return Err(ClusterError::InvalidReference {
            message: "subscribe topic must not be empty".into(),
        });
    }
    reject_reserved_routing_key(routing_key.as_deref())?;
    let pattern = render_subject_pattern(&topic, routing_key.as_deref());
    let sub = match group {
        Some(g) => nats.queue_subscribe(pattern.clone(), g).await,
        None => nats.subscribe(pattern.clone()).await,
    }
    .map_err(|e| ClusterError::BackendUnavailable {
        reason: format!("nats subscribe {pattern}: {e}"),
    })?;

    // Map each `async_nats::Message` to a `PublishedMessage` with
    // routing-key + from_node populated, mirroring the FFI shim.
    let topic_owned = topic;
    let stream = sub.map(move |msg| {
        let routing = recover_routing_key(&topic_owned, msg.subject.as_str());
        let from_node = msg
            .headers
            .as_ref()
            .and_then(|h| h.get("X-Mcpg-From-Node"))
            .map(|v| v.to_string())
            .unwrap_or_default();
        PublishedMessage {
            topic: topic_owned.clone(),
            routing_key: routing,
            payload: msg.payload,
            from_node,
        }
    });
    Ok(Box::pin(stream))
}

/// Async stream-returning watch_peers used by the in-process
/// `ClusterBackend` impl. Wraps the heartbeat broadcast
/// channel via `BroadcastStream`; lagged events are logged at
/// `warn` and skipped (matching the FFI shim's behaviour).
pub(crate) fn watch_peers_stream(rx: broadcast::Receiver<PeerEvent>) -> BoxPeerEventStream {
    use tokio_stream::wrappers::BroadcastStream;
    let stream = BroadcastStream::new(rx).filter_map(|r| async move {
        match r {
            Ok(evt) => Some(evt),
            Err(tokio_stream::wrappers::errors::BroadcastStreamRecvError::Lagged(n)) => {
                tracing::warn!(
                    skipped = n,
                    "nats cluster: watch_peers stream lagged; events skipped"
                );
                None
            }
        }
    });
    Box::pin(stream)
}

fn recover_routing_key(topic: &str, subject: &str) -> Option<String> {
    let prefix = format!("{NOTIFY_SUBJECT_PREFIX}.{topic}.");
    let stripped = subject.strip_prefix(&prefix)?;
    if stripped.is_empty() || stripped == DEFAULT_ROUTING_KEY {
        // Empty (legacy 3-token subject) or the no-routing-key
        // sentinel both round-trip to `None`.
        None
    } else {
        Some(stripped.to_owned())
    }
}

/// Spawn a watch_peers forwarder. Subscribes to the broadcast
/// channel the heartbeat subscriber + sweeper publish on; each
/// received `PeerEvent` is JSON-encoded and pushed through
/// `emit_event`.
pub(crate) fn watch_peers(
    runtime: &Handle,
    rx: broadcast::Receiver<PeerEvent>,
    emit_event: Box<dyn Fn(&str) + Send + Sync + 'static>,
) -> Result<WatchHandleBox, ClusterError> {
    let (done_tx, done_rx) = std::sync::mpsc::channel::<()>();
    let join = runtime.spawn(async move {
        // See subscribe(): fires on drop/abort so StreamState::drop can
        // wait for forwarder quiescence before the host frees the bridge.
        let _done = ForwarderDoneSignal(done_tx);
        let mut rx = rx;
        loop {
            match rx.recv().await {
                Ok(event) => match serde_json::to_string(&event) {
                    Ok(s) => emit_event(&s),
                    Err(e) => {
                        tracing::warn!(error = %e, "nats cluster: PeerEvent serialise failed");
                    }
                },
                // `Lagged` only happens if the watcher fell behind.
                // Keep going — the next event is likely fresher.
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    tracing::warn!(skipped = n, "nats cluster: watch_peers lagged");
                    continue;
                }
                Err(broadcast::error::RecvError::Closed) => return,
            }
        }
    });
    let state = Box::new(StreamState {
        abort: join.abort_handle(),
        done_rx,
    });
    Ok(WatchHandleBox(Box::into_raw(state) as *mut ()))
}

/// `cancel_stream` / drop-time reclaim — symmetric with the lease
/// drop path. Idempotent on null.
pub(crate) unsafe fn drop_stream(handle: WatchHandleBox) {
    if handle.0.is_null() {
        return;
    }
    // SAFETY: per the host vtable contract, the pointer was
    // produced by `subscribe` / `watch_peers` and hasn't been
    // reclaimed yet.
    unsafe {
        let _ = Box::from_raw(handle.0 as *mut StreamState);
    }
}

/// Convenience for the lib root: a clone of an `Arc<broadcast::
/// Sender>`. Used by `peer.rs` to publish events into the same
/// channel `watch_peers` consumes.
pub(crate) type PeerEventSender = Arc<broadcast::Sender<PeerEvent>>;

/// Construct a (sender, _receiver_factory) pair. The plugin holds
/// the sender + a 0-capacity receiver placeholder; each
/// `watch_peers` call mints a fresh receiver via
/// `Sender::subscribe()`.
pub(crate) fn new_peer_event_channel(capacity: usize) -> PeerEventSender {
    let (tx, _rx) = broadcast::channel(capacity);
    Arc::new(tx)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn render_subject_pads_no_routing_key_with_sentinel() {
        // Both no-key and explicit-empty-string round-trip to the
        // sentinel to keep the wire arity stable at 4 tokens.
        assert_eq!(
            render_subject("orders", None),
            format!("mcpg.notify.orders.{DEFAULT_ROUTING_KEY}")
        );
        assert_eq!(
            render_subject("orders", Some("")),
            format!("mcpg.notify.orders.{DEFAULT_ROUTING_KEY}")
        );
    }

    #[test]
    fn render_subject_with_routing_key_uses_it() {
        assert_eq!(
            render_subject("orders", Some("placed")),
            "mcpg.notify.orders.placed"
        );
    }

    #[test]
    fn render_subscribe_pattern_uses_single_token_wildcard() {
        // `*` matches exactly one routing-key token, including the
        // DEFAULT_ROUTING_KEY sentinel — which is how subscribe(None)
        // matches publish(None).
        assert_eq!(
            render_subject_pattern("orders", None),
            "mcpg.notify.orders.*"
        );
        assert_eq!(
            render_subject_pattern("orders", Some("placed")),
            "mcpg.notify.orders.placed"
        );
    }

    #[test]
    fn recover_routing_key_strips_sentinel() {
        assert_eq!(
            recover_routing_key("orders", "mcpg.notify.orders._default_"),
            None
        );
        assert_eq!(
            recover_routing_key("orders", "mcpg.notify.orders.placed"),
            Some("placed".to_owned())
        );
        // Legacy 3-token subjects (pre-this-fix) round-trip to
        // None too, for forward-compat with any persisted
        // pre-fix messages JetStream might still be replaying.
        assert_eq!(recover_routing_key("orders", "mcpg.notify.orders"), None);
    }

    #[test]
    fn reserved_routing_key_rejected_at_publish_and_subscribe_time() {
        // The validator runs before the wire call — any operator
        // sending DEFAULT_ROUTING_KEY directly hits a clear error
        // pointing at the collision rather than a silent
        // round-trip-as-None surprise.
        let err = reject_reserved_routing_key(Some(DEFAULT_ROUTING_KEY)).unwrap_err();
        match err {
            ClusterError::InvalidReference { message } => {
                assert!(message.contains(DEFAULT_ROUTING_KEY));
                assert!(message.contains("reserved"));
            }
            other => panic!("expected InvalidReference, got {other:?}"),
        }
        // Empty / None / any other key passes.
        reject_reserved_routing_key(None).unwrap();
        reject_reserved_routing_key(Some("placed")).unwrap();
        reject_reserved_routing_key(Some("")).unwrap();
    }

    /// Regression: StreamState::drop must not return until the
    /// forwarder future has actually been dropped — otherwise an in-flight
    /// `emit_event` could touch the host's StreamBridge after the host frees
    /// it (use-after-free). We prove the ordering with a Drop flag on a
    /// task-local: after `drop(state)` returns, the flag must already be set.
    #[test]
    fn stream_state_drop_waits_for_forwarder_to_quiesce() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::time::Duration;

        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();

        struct DropFlag(Arc<AtomicBool>);
        impl Drop for DropFlag {
            fn drop(&mut self) {
                self.0.store(true, Ordering::SeqCst);
            }
        }

        let forwarder_dropped = Arc::new(AtomicBool::new(false));
        let flag_for_task = Arc::clone(&forwarder_dropped);

        let (done_tx, done_rx) = std::sync::mpsc::channel::<()>();
        let (entered_tx, entered_rx) = std::sync::mpsc::channel::<()>();

        let join = rt.handle().spawn(async move {
            // Order matters: ForwarderDoneSignal is created first so it drops
            // LAST — the done signal fires only after DropFlag (and any real
            // forwarder locals) have been dropped.
            let _done = ForwarderDoneSignal(done_tx);
            let _flag = DropFlag(flag_for_task);
            entered_tx.send(()).unwrap();
            // Park at an await; abort takes effect here.
            loop {
                tokio::time::sleep(Duration::from_secs(3600)).await;
            }
        });

        // Wait until the forwarder is actually running.
        entered_rx.recv().unwrap();
        assert!(!forwarder_dropped.load(Ordering::SeqCst));

        // Drop runs on this (non-runtime) thread, so the bounded blocking
        // wait is safe; the runtime drops the aborted future on a worker.
        let state = StreamState {
            abort: join.abort_handle(),
            done_rx,
        };
        drop(state);

        assert!(
            forwarder_dropped.load(Ordering::SeqCst),
            "StreamState::drop returned before the forwarder future was dropped (quiescence)"
        );
    }
}
