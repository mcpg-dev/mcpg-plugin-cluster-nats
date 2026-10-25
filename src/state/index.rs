//! The key index of a state bucket.
//!
//! A data key is one JetStream subject token (`encode_key` escapes every
//! separator), so the server cannot filter a listing by prefix. Next to
//! each data record the bucket holds an index record whose subject splits
//! the key into tokens after each `:`, `/` and `.` — the separator stays,
//! escaped, at the end of its token — so the prefix `sub:u:abc:` is the
//! server-side filter `=I.sub=3A.u=3A.abc=3A.>`.
//!
//! Index keys start with `=I`, and the index-state key with `=M`: `=`
//! followed by a letter past `F` is never output of `encode_key`, so neither
//! can be read as a data key, also by an older build.

/// The first token of every index key.
const INDEX_TOKEN: &str = "=I";

/// The last token of an index key whose data key ends with a separator.
const END_TOKEN: &str = "=Z";

/// The key that records when the index was last made complete.
pub(crate) const INDEX_STATE_KEY: &str = "=M.index-complete";

/// True for a separator a token ends at.
fn is_separator(b: u8) -> bool {
    matches!(b, b':' | b'/' | b'.')
}

/// Escape one byte of a token: `A-Za-z0-9`, `-` and `_` pass, every other
/// byte (separators included) becomes `=HH`.
fn push_escaped(out: &mut String, b: u8) {
    if b.is_ascii_alphanumeric() || b == b'-' || b == b'_' {
        out.push(b as char);
    } else {
        out.push('=');
        out.push_str(&format!("{b:02X}"));
    }
}

/// The index tokens of `key` as `.`-joined text, without the leading
/// [`INDEX_TOKEN`], and whether `key` ended with a separator.
fn tokens(key: &str) -> (String, bool) {
    let mut out = String::with_capacity(key.len() * 2);
    let mut open = false;
    for &b in key.as_bytes() {
        if !open && !out.is_empty() {
            out.push('.');
        }
        push_escaped(&mut out, b);
        open = !is_separator(b);
    }
    (out, !open)
}

/// The index key of the data key `key`.
pub(crate) fn index_key(key: &str) -> String {
    let (tokens, ends_at_separator) = tokens(key);
    match (tokens.is_empty(), ends_at_separator) {
        (true, _) => format!("{INDEX_TOKEN}.{END_TOKEN}"),
        (false, true) => format!("{INDEX_TOKEN}.{tokens}.{END_TOKEN}"),
        (false, false) => format!("{INDEX_TOKEN}.{tokens}"),
    }
}

/// The data key an index key stands for; `None` for any other key.
pub(crate) fn data_key(index_key: &str) -> Option<String> {
    let rest = index_key.strip_prefix(INDEX_TOKEN)?.strip_prefix('.')?;
    let mut out: Vec<u8> = Vec::with_capacity(rest.len());
    let mut tokens = rest.split('.').peekable();
    while let Some(token) = tokens.next() {
        if token == END_TOKEN && tokens.peek().is_none() {
            break;
        }
        let bytes = token.as_bytes();
        let mut i = 0;
        while i < bytes.len() {
            if bytes[i] == b'=' {
                let hex = token.get(i + 1..i + 3)?;
                out.push(u8::from_str_radix(hex, 16).ok()?);
                i += 3;
            } else {
                out.push(bytes[i]);
                i += 1;
            }
        }
    }
    String::from_utf8(out).ok()
}

/// True for a key of the index (an index record or the index state).
pub(crate) fn is_index_key(key: &str) -> bool {
    key.starts_with("=I.") || key.starts_with("=M.")
}

/// The index-key filter that holds every key under `prefix`, after the
/// bucket's subject prefix: the prefix's whole tokens followed by `>`. A
/// prefix that ends inside a token matches more on the server than it
/// should, and the caller checks each key against the prefix.
pub(crate) fn prefix_filter(prefix: &str) -> String {
    let whole = match prefix.as_bytes().iter().rposition(|&b| is_separator(b)) {
        Some(last) => &prefix[..=last],
        None => "",
    };
    let (tokens, _) = tokens(whole);
    if tokens.is_empty() {
        format!("{INDEX_TOKEN}.>")
    } else {
        format!("{INDEX_TOKEN}.{tokens}.>")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Mirror of the subject-token rules async-nats checks for a KV key.
    fn is_valid_kv_key(key: &str) -> bool {
        !key.is_empty()
            && !key.starts_with('.')
            && !key.ends_with('.')
            && !key.contains("..")
            && key
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'/' | b'_' | b'=' | b'.'))
    }

    #[test]
    fn index_keys_round_trip_and_are_valid_subjects() {
        for key in [
            "session:abc",
            "sub:u:aHR0cDovL3g=:sess-1",
            "mcpg.cancel.pending.r-7",
            "request_state/handle.7",
            "idempotency/ab12/key with space",
            "a::b",
            ":lead",
            "trail:",
            "x",
            "unicode-✓:k",
            "=3D:literal-escape",
            "...",
        ] {
            let ik = index_key(key);
            assert!(
                is_valid_kv_key(&ik),
                "`{ik}` (from `{key}`) is not a valid KV key"
            );
            assert!(is_index_key(&ik));
            assert_eq!(data_key(&ik).as_deref(), Some(key), "via `{ik}`");
        }
    }

    #[test]
    fn a_whole_token_prefix_filters_on_the_server() {
        assert_eq!(prefix_filter("session:"), "=I.session=3A.>");
        assert_eq!(prefix_filter("sub:u:abc:"), "=I.sub=3A.u=3A.abc=3A.>");
        assert_eq!(
            prefix_filter("mcpg.cancel.pending."),
            "=I.mcpg=2E.cancel=2E.pending=2E.>"
        );
        assert!(
            index_key("session:abc").starts_with(prefix_filter("session:").trim_end_matches('>'))
        );
    }

    #[test]
    fn a_partial_token_prefix_widens_to_its_whole_tokens() {
        assert_eq!(prefix_filter("sub:u:ab"), "=I.sub=3A.u=3A.>");
        assert_eq!(prefix_filter("sess"), "=I.>");
        assert_eq!(prefix_filter(""), "=I.>");
    }

    #[test]
    fn a_sibling_family_is_outside_the_filter() {
        let filter = prefix_filter("a:");
        let head = filter.trim_end_matches('>');
        assert!(index_key("a:1").starts_with(head));
        assert!(!index_key("ab:1").starts_with(head));
        assert!(!index_key("b:1").starts_with(head));
    }

    #[test]
    fn data_keys_and_index_keys_never_mix() {
        // `encode_key` output never starts with `=` followed by a letter
        // past `F`.
        assert!(!is_index_key("session=3Aabc"));
        assert!(!is_index_key("=3Alead"));
        assert_eq!(data_key("session=3Aabc"), None);
        assert_eq!(data_key(INDEX_STATE_KEY), None);
    }
}
