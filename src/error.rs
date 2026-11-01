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

/// Whether a failed write lost a compare-and-set: the subject held another
/// last sequence than the write expected. The server answers this with
/// error code 10071 before 2.14 and 10164 from 2.14, and the client sorts
/// only 10071 into its "wrong last sequence" kinds, so the code is read
/// from the server's error in the source chain.
pub(crate) fn is_wrong_last_sequence(err: &(dyn std::error::Error + 'static)) -> bool {
    use async_nats::jetstream::ErrorCode;
    let mut next = Some(err);
    while let Some(e) = next {
        if let Some(js) = e.downcast_ref::<async_nats::jetstream::Error>() {
            let code = js.error_code();
            return code == ErrorCode::STREAM_WRONG_LAST_SEQUENCE
                || code == ErrorCode::STREAM_WRONG_LAST_SEQUENCE_CONSTANT;
        }
        next = e.source();
    }
    false
}

/// Translate a `serde_json` decode failure on a JS KV value into
/// [`ClusterError::Internal`]. Used wherever the plugin reads a
/// lease record back out of the bucket.
pub(crate) fn json_decode_to_cluster(op: &'static str, err: serde_json::Error) -> ClusterError {
    ClusterError::Internal {
        reason: format!("nats {op}: KV value decode: {err}"),
    }
}
