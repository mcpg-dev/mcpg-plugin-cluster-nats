//! NATS JetStream-backed cluster-api primitive implementations.
//!
//! Internal sub-module of `mcpg-plugin-cluster-nats`; assembles
//! these primitives over the single shared NATS connection owned
//! by the cluster plugin.
//!
//! Implements:
//! - [`NatsKv`] — `KeyValueStore` over a JetStream KV bucket, with
//!   a revision-CAS-loop atomic `incr`
//! - [`NatsTopicBus`] — `PubSub` over Core NATS subjects

mod kv;
mod topic;

pub use kv::NatsKv;
pub use topic::NatsTopicBus;
