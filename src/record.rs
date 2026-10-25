//! Reads of a KV bucket's last record of a key, from the stream's leader.
//!
//! The KV client reads with direct gets, which any replica of the stream
//! answers, also one that has not applied the latest write yet: a read on
//! one gateway could miss a write that another gateway saw acknowledged. A
//! `STREAM.MSG.GET` is answered by the leader, which acknowledged that
//! write.

use async_nats::jetstream::kv::{Operation, Store as KvStore};
use async_nats::jetstream::stream::LastRawMessageErrorKind;
use async_nats::{HeaderMap, HeaderValue};
use bytes::Bytes;

/// The header of a KV delete or purge marker.
const KV_OPERATION_HEADER: &str = "KV-Operation";

/// The last record of a key.
pub(crate) struct Record {
    pub(crate) revision: u64,
    pub(crate) operation: Operation,
    pub(crate) payload: Bytes,
}

/// The last record of `key` in `store`, or `None` when the key has none.
pub(crate) async fn last_record(store: &KvStore, key: &str) -> Result<Option<Record>, String> {
    let subject = format!("{}{key}", store.prefix);
    match store.stream.get_last_raw_message_by_subject(&subject).await {
        Ok(message) => Ok(Some(Record {
            revision: message.sequence,
            operation: record_operation(&message.headers),
            payload: message.payload,
        })),
        Err(e) if e.kind() == LastRawMessageErrorKind::NoMessageFound => Ok(None),
        Err(e) => Err(e.to_string()),
    }
}

/// A record's KV operation, read as the KV client reads it: a put carries
/// no operation header.
fn record_operation(headers: &HeaderMap) -> Operation {
    if let Some(operation) = headers.get(KV_OPERATION_HEADER) {
        return operation.as_str().parse().unwrap_or(Operation::Put);
    }
    match headers
        .get(async_nats::header::NATS_MARKER_REASON)
        .map(HeaderValue::as_str)
    {
        Some("MaxAge" | "Purge") => Operation::Purge,
        Some("Remove") => Operation::Delete,
        _ => Operation::Put,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn headers(pairs: &[(&str, &str)]) -> HeaderMap {
        let mut map = HeaderMap::new();
        for (name, value) in pairs {
            map.insert(*name, *value);
        }
        map
    }

    #[test]
    fn a_record_reads_as_the_kv_client_reads_it() {
        assert_eq!(record_operation(&headers(&[])), Operation::Put);
        assert_eq!(
            record_operation(&headers(&[("Mcpg-Expires", "0")])),
            Operation::Put
        );
        assert_eq!(
            record_operation(&headers(&[(KV_OPERATION_HEADER, "DEL")])),
            Operation::Delete
        );
        assert_eq!(
            record_operation(&headers(&[(KV_OPERATION_HEADER, "PURGE")])),
            Operation::Purge
        );
        assert_eq!(
            record_operation(&headers(&[("Nats-Marker-Reason", "MaxAge")])),
            Operation::Purge
        );
        assert_eq!(
            record_operation(&headers(&[("Nats-Marker-Reason", "Remove")])),
            Operation::Delete
        );
    }
}
