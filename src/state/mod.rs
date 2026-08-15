//! NATS JetStream-backed cluster-api primitive implementations.
//!
//! Internal sub-module of `mcpg-plugin-cluster-nats`; assembles
//! these primitives over the single shared NATS connection owned
//! by the cluster plugin.
//!
//! Implements:
//! - [`NatsKv`] — `KeyValueStore` over a JetStream KV bucket
//! - [`NatsLock`] — `Lease` via JS KV CAS with monotonic fence tokens
//! - [`NatsTopicBus`] — `PubSub` over Core NATS subjects
//! - [`NatsWatch`] — `Watch` over JS KV's native `watch_all` stream

mod kv;
mod lock;
mod topic;
mod watch;

pub use kv::NatsKv;
pub use lock::NatsLock;
pub use topic::NatsTopicBus;
pub use watch::NatsWatch;
