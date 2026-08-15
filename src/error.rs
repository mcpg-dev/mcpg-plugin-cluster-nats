//! Config errors are local; runtime NATS / JS errors translate to
//! [`ClusterError`].

use mcpg_cluster_api::ClusterError;
use thiserror::Error;

/// Failures while parsing the operator-supplied config blob.
#[derive(Debug, Clone, Error)]
pub enum ConfigError {
    #[error("nats cluster config: failed to parse JSON: {0}")]
    ParseError(String),

    #[error("nats cluster config: invalid: {0}")]
    Invalid(String),
}

/// Translate a `serde_json` decode failure on a JS KV value into
/// [`ClusterError::Internal`]. Used wherever the plugin reads a
/// lease record back out of the bucket.
pub(crate) fn json_decode_to_cluster(op: &'static str, err: serde_json::Error) -> ClusterError {
    ClusterError::Internal {
        reason: format!("nats {op}: KV value decode: {err}"),
    }
}
