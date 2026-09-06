//! Operator-supplied configuration for the NATS JetStream cluster
//! coordinator.

use serde::{Deserialize, Serialize};

use crate::error::ConfigError;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClusterNatsConfig {
    /// One or more NATS server URLs. The async-nats client load-
    /// balances + reconnects across them.
    pub servers: Vec<String>,

    /// Optional auth. Tagged on `method` so the operator picks one.
    #[serde(default)]
    pub auth: Option<AuthConfig>,

    /// Optional TLS knobs. TLS is REQUIRED BY DEFAULT (secure-by-default):
    /// with this block omitted the connection still demands TLS. To run
    /// plaintext NATS (local/dev), set `tls: { require_tls: false }`
    /// explicitly. `ca_cert` trusts a private-CA root (inline PEM or a
    /// file path).
    #[serde(default)]
    pub tls: Option<TlsConfig>,

    /// JetStream knobs.
    #[serde(default)]
    pub jetstream: JetStreamConfig,

    /// Local node identity + heartbeat knobs.
    pub node: NodeConfig,

    /// Default lease TTL knobs.
    #[serde(default)]
    pub lease: LeaseConfig,

    /// Connection-level timeouts.
    #[serde(default)]
    pub connection: ConnectionConfig,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "method", rename_all = "snake_case")]
pub enum AuthConfig {
    /// Static token.
    Token { token: String },
    /// Username + password.
    UserPassword { user: String, password: String },
    /// `.creds` file path. async-nats reads it on connect.
    CredentialsFile { path: String },
    /// Inline NATS credentials: the full `.creds` file CONTENT (the
    /// user-JWT and nkey-seed sections), typically injected as
    /// `${env.VAR}` via the gateway's config-load substitution so no
    /// credentials file has to be mounted. Shape-checked at config
    /// load; the value itself is never logged.
    Credentials { creds: String },
}

/// Section markers a standard NATS credentials file carries. The early
/// shape check on inline `auth.creds` pivots on them.
const CREDS_JWT_MARKER: &str = "-----BEGIN NATS USER JWT-----";
const CREDS_SEED_MARKER: &str = "-----BEGIN USER NKEY SEED-----";

impl std::fmt::Debug for AuthConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Manual impl so credential material (token, password, creds
        // blob) is never rendered into a log line / panic message.
        // Non-secret selector fields stay visible.
        match self {
            Self::Token { .. } => f.debug_struct("Token").field("token", &"***").finish(),
            Self::UserPassword { user, .. } => f
                .debug_struct("UserPassword")
                .field("user", user)
                .field("password", &"***")
                .finish(),
            Self::CredentialsFile { path } => f
                .debug_struct("CredentialsFile")
                .field("path", path)
                .finish(),
            Self::Credentials { .. } => f
                .debug_struct("Credentials")
                .field("creds", &"***")
                .finish(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TlsConfig {
    /// Private-CA root certificate (PEM, inline or path), matching the
    /// redis coordinator convention: a value starting with
    /// `-----BEGIN` is inline PEM content, anything else is a
    /// filesystem path. When set it becomes the trust root in place of
    /// the system roots; server-cert verification stays on.
    #[serde(default)]
    pub ca_cert: Option<String>,
    /// Whether TLS is required for the connection (maps to async-nats
    /// `ConnectOptions::require_tls`). Defaults to TRUE. async-nats has no
    /// API to disable server-cert verification, so TLS, when negotiated,
    /// is always rustls-verified.
    #[serde(default = "default_require_tls")]
    pub require_tls: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JetStreamConfig {
    /// Optional JS domain (NATS leaf-node deploys segment by domain).
    /// Sent via `with_jetstream_domain` on the `jetstream::Context`.
    #[serde(default)]
    pub domain: Option<String>,

    /// KV bucket name for leases + locks. Created at boot if missing.
    #[serde(default = "default_leases_bucket")]
    pub leases_bucket: String,

    /// KV bucket name for fencing-token counters. Created at boot
    /// if missing. Stores monotonic counters keyed on the lease/lock name.
    #[serde(default = "default_fencing_bucket")]
    pub fencing_bucket: String,

    /// Stream name for pub/sub fan-out.
    #[serde(default = "default_notifications_stream")]
    pub notifications_stream: String,

    /// KV bucket name for the `KeyValueStore` primitive surface
    /// the gateway's session / pipeline / task / subscription
    /// stores consume by inheriting from `cluster.kind: nats`.
    /// Distinct from `leases_bucket` / `fencing_bucket` so the
    /// coordinator's own coordination state can't collide with
    /// capability state. Created at boot if missing.
    #[serde(default = "default_state_bucket")]
    pub state_bucket: String,

    /// Replication factor for created KV buckets + streams. Default 1
    /// works on single-node dev NATS; production deploys with a
    /// 3+ node cluster should set 3.
    #[serde(default = "default_replicas")]
    pub replicas: u32,

    /// Storage backend for the KV buckets the plugin creates.
    /// `file` (default for prod) keeps lease state across NATS
    /// restarts; `memory` is faster + lighter but loses state on
    /// any NATS server restart.
    ///
    /// Use `memory` only when (a) the lease state itself is
    /// disposable (e.g. dev / staging) or (b) operators
    /// explicitly accept the trade-off in exchange for simpler
    /// JS configuration (no `--store_dir` write-permission
    /// requirements on the NATS pod). Production deploys should
    /// stay on `file`.
    #[serde(default = "default_storage")]
    pub storage: KvStorage,
}

/// JetStream KV storage backend selector.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum KvStorage {
    /// In-memory storage. Lease state lost on any NATS restart.
    Memory,
    /// Disk-backed storage. Lease state survives NATS restarts.
    /// Requires the NATS server to have a writable `--store_dir`.
    File,
}

impl Default for JetStreamConfig {
    fn default() -> Self {
        Self {
            domain: None,
            leases_bucket: default_leases_bucket(),
            fencing_bucket: default_fencing_bucket(),
            notifications_stream: default_notifications_stream(),
            state_bucket: default_state_bucket(),
            replicas: default_replicas(),
            storage: default_storage(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeConfig {
    /// Unique per gateway replica. Operators typically pin to the
    /// hostname or pod name.
    pub id: String,

    /// Optional advertised address (peers store this for follow-up
    /// connectivity reporting). Not used for routing today.
    #[serde(default)]
    pub address: Option<String>,

    /// Heartbeat publish cadence in seconds.
    #[serde(default = "default_heartbeat_interval_sec")]
    pub heartbeat_interval_sec: u64,

    /// Peer is reclassified Unreachable after this many seconds
    /// without a fresh heartbeat. Bumped above `heartbeat_interval`
    /// to absorb network jitter.
    #[serde(default = "default_peer_expiry_sec")]
    pub peer_expiry_sec: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LeaseConfig {
    /// Default TTL (seconds) when the caller passes 0 to
    /// `acquire_leadership` / `acquire_lock`. The trait uses
    /// milliseconds; operator config is in seconds for sanity.
    #[serde(default = "default_lease_ttl_sec")]
    pub default_ttl_sec: u64,

    /// When the renewal task wakes. Expressed as a percentage of
    /// the TTL — e.g. 50 → renew at half the TTL.
    #[serde(default = "default_renew_before_expiry_percent")]
    pub renew_before_expiry_percent: u32,
}

impl Default for LeaseConfig {
    fn default() -> Self {
        Self {
            default_ttl_sec: default_lease_ttl_sec(),
            renew_before_expiry_percent: default_renew_before_expiry_percent(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConnectionConfig {
    #[serde(default = "default_connect_timeout_ms")]
    pub connect_timeout_ms: u64,
    #[serde(default = "default_operation_timeout_ms")]
    pub operation_timeout_ms: u64,
}

impl Default for ConnectionConfig {
    fn default() -> Self {
        Self {
            connect_timeout_ms: default_connect_timeout_ms(),
            operation_timeout_ms: default_operation_timeout_ms(),
        }
    }
}

/// Detect whether a PEM-ish config value is inline content rather than
/// a filesystem path, matching the redis coordinator convention.
pub(crate) fn is_inline_pem(value: &str) -> bool {
    value.trim_start().starts_with("-----BEGIN")
}

fn default_require_tls() -> bool {
    true
}
fn default_leases_bucket() -> String {
    "mcpg-leases".into()
}
fn default_fencing_bucket() -> String {
    "mcpg-fencing".into()
}
fn default_notifications_stream() -> String {
    "mcpg-notifications".into()
}
fn default_state_bucket() -> String {
    "mcpg-state".into()
}
fn default_replicas() -> u32 {
    1
}
fn default_storage() -> KvStorage {
    // File-backed by default — production durability across NATS
    // restarts. Dev / test deployments override to `memory` to
    // sidestep the JS store-dir write-permission requirement.
    KvStorage::File
}
fn default_heartbeat_interval_sec() -> u64 {
    10
}
fn default_peer_expiry_sec() -> u64 {
    30
}
fn default_lease_ttl_sec() -> u64 {
    30
}
fn default_renew_before_expiry_percent() -> u32 {
    50
}
fn default_connect_timeout_ms() -> u64 {
    5_000
}
fn default_operation_timeout_ms() -> u64 {
    10_000
}

impl ClusterNatsConfig {
    pub fn parse(config_json: &str) -> Result<Self, ConfigError> {
        let cfg: Self = serde_json::from_str(config_json)
            .map_err(|e| ConfigError::ParseError(e.to_string()))?;
        cfg.validate()?;
        Ok(cfg)
    }

    fn validate(&self) -> Result<(), ConfigError> {
        if self.servers.is_empty() {
            return Err(ConfigError::Invalid(
                "`servers` must contain at least one URL".into(),
            ));
        }
        for s in &self.servers {
            let trimmed = s.trim();
            if trimmed.is_empty() {
                return Err(ConfigError::Invalid(
                    "`servers` entries must be non-empty".into(),
                ));
            }
            if !(trimmed.starts_with("nats://")
                || trimmed.starts_with("tls://")
                || trimmed.starts_with("nats+tls://")
                || trimmed.starts_with("ws://")
                || trimmed.starts_with("wss://"))
            {
                return Err(ConfigError::Invalid(format!(
                    "`servers` URL must use scheme nats:// / tls:// / nats+tls:// / ws:// / wss:// — got `{trimmed}`"
                )));
            }
        }
        if let Some(AuthConfig::Credentials { creds }) = &self.auth {
            // Shape check only — async-nats parses the JWT + seed for
            // real when the connect options are built. Failing here
            // turns a half-pasted value / wrong env var into a
            // config-load error instead of a connect-time one. The
            // message never echoes the value: it is a credential.
            for marker in [CREDS_JWT_MARKER, CREDS_SEED_MARKER] {
                if !creds.contains(marker) {
                    return Err(ConfigError::Invalid(format!(
                        "`auth.creds` must be the full NATS credentials-file content, but its \
                         `{marker}` section is missing (value not echoed — check the env var / \
                         secret it was substituted from)"
                    )));
                }
            }
        }
        let node_id = self.node.id.trim();
        if node_id.is_empty() {
            return Err(ConfigError::Invalid(
                "`node.id` must not be empty (use the gateway pod / hostname)".into(),
            ));
        }
        // The runtime publishes on a subject built from the UNtrimmed id, so
        // surrounding whitespace would form an invalid subject token even
        // though the char scan below runs on the trimmed value.
        if self.node.id != node_id {
            return Err(ConfigError::Invalid(
                "`node.id` must not have leading or trailing whitespace".into(),
            ));
        }
        // node.id is the trailing token of the heartbeat subject
        // `mcpg.peers.heartbeat.<id>`, so it must be a single, well-formed
        // NATS subject token: no `.` (token separator), no `*`/`>` wildcards,
        // and no whitespace / control chars.
        if let Some(bad) = node_id
            .chars()
            .find(|c| matches!(c, '.' | '*' | '>') || c.is_whitespace() || c.is_control())
        {
            return Err(ConfigError::Invalid(format!(
                "`node.id` must be a single NATS subject token — it is the trailing token of the \
                 heartbeat subject `mcpg.peers.heartbeat.<id>`. Remove the {bad:?} character \
                 (no `.`, `*`, `>`, whitespace, or control chars)."
            )));
        }
        if self.node.heartbeat_interval_sec == 0 {
            return Err(ConfigError::Invalid(
                "`node.heartbeat_interval_sec` must be > 0".into(),
            ));
        }
        if self.node.peer_expiry_sec <= self.node.heartbeat_interval_sec {
            return Err(ConfigError::Invalid(
                "`node.peer_expiry_sec` must be > `heartbeat_interval_sec` so peers \
                 don't flicker on a single missed beat"
                    .into(),
            ));
        }
        if self.lease.default_ttl_sec == 0 {
            return Err(ConfigError::Invalid(
                "`lease.default_ttl_sec` must be > 0".into(),
            ));
        }
        if !(1..=99).contains(&self.lease.renew_before_expiry_percent) {
            return Err(ConfigError::Invalid(
                "`lease.renew_before_expiry_percent` must be in 1..=99".into(),
            ));
        }
        if self.jetstream.replicas == 0 {
            return Err(ConfigError::Invalid(
                "`jetstream.replicas` must be > 0 (use 1 for single-node dev, 3 for prod)".into(),
            ));
        }
        for (label, name) in [
            ("jetstream.leases_bucket", &self.jetstream.leases_bucket),
            ("jetstream.fencing_bucket", &self.jetstream.fencing_bucket),
            (
                "jetstream.notifications_stream",
                &self.jetstream.notifications_stream,
            ),
        ] {
            if name.trim().is_empty() {
                return Err(ConfigError::Invalid(format!("`{label}` must not be empty")));
            }
        }
        if self.connection.connect_timeout_ms == 0 || self.connection.operation_timeout_ms == 0 {
            return Err(ConfigError::Invalid(
                "`connection.*_timeout_ms` must be > 0".into(),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn minimal() -> &'static str {
        r#"{
            "servers": ["nats://nats:4222"],
            "node": {"id": "gateway-1"}
        }"#
    }

    #[test]
    fn minimal_parses_with_defaults() {
        let cfg = ClusterNatsConfig::parse(minimal()).unwrap();
        assert_eq!(cfg.servers, vec!["nats://nats:4222".to_string()]);
        assert_eq!(cfg.node.id, "gateway-1");
        assert_eq!(cfg.jetstream.leases_bucket, "mcpg-leases");
        assert_eq!(cfg.jetstream.fencing_bucket, "mcpg-fencing");
        assert_eq!(cfg.jetstream.replicas, 1);
        assert_eq!(cfg.lease.default_ttl_sec, 30);
        assert_eq!(cfg.lease.renew_before_expiry_percent, 50);
        assert_eq!(cfg.node.heartbeat_interval_sec, 10);
        assert_eq!(cfg.node.peer_expiry_sec, 30);
    }

    #[test]
    fn empty_servers_list_rejected() {
        let err = ClusterNatsConfig::parse(r#"{"servers": [], "node": {"id": "g1"}}"#).unwrap_err();
        assert!(err.to_string().contains("servers"));
    }

    fn parse_with_node_id(id: &str) -> Result<ClusterNatsConfig, ConfigError> {
        ClusterNatsConfig::parse(&format!(
            r#"{{"servers": ["nats://nats:4222"], "node": {{"id": "{id}"}}}}"#
        ))
    }

    #[test]
    fn node_id_with_dot_rejected() {
        let err = parse_with_node_id("gw.1").unwrap_err();
        assert!(err.to_string().contains("node.id"), "{err}");
    }

    #[test]
    fn node_id_with_wildcard_rejected() {
        assert!(parse_with_node_id("gw*").is_err());
        assert!(parse_with_node_id("gw>").is_err());
    }

    #[test]
    fn node_id_with_whitespace_rejected() {
        assert!(parse_with_node_id("gw 1").is_err());
    }

    #[test]
    fn node_id_with_surrounding_whitespace_rejected() {
        // Leading/trailing whitespace is trimmed before the char scan, so it
        // must be caught separately — the runtime publishes on the untrimmed id.
        assert!(parse_with_node_id(" gw1").is_err());
        assert!(parse_with_node_id("gw1 ").is_err());
    }

    #[test]
    fn plain_hostname_node_id_accepted() {
        // Regression guard: the typical pod/hostname charset still parses.
        assert!(parse_with_node_id("gateway-1_pod").is_ok());
    }

    #[test]
    fn tls_block_defaults_require_tls_true() {
        // Secure-by-default — an empty tls block still requires TLS.
        let cfg = ClusterNatsConfig::parse(
            r#"{"servers": ["nats://nats:4222"], "node": {"id": "g1"}, "tls": {}}"#,
        )
        .unwrap();
        assert!(cfg.tls.unwrap().require_tls);
    }

    #[test]
    fn tls_require_tls_false_is_explicit_opt_out() {
        let cfg = ClusterNatsConfig::parse(
            r#"{"servers": ["nats://nats:4222"], "node": {"id": "g1"}, "tls": {"require_tls": false}}"#,
        )
        .unwrap();
        assert!(!cfg.tls.unwrap().require_tls);
    }

    #[test]
    fn stale_verify_peer_field_now_rejected() {
        // The former `verify_peer` knob is gone; deny_unknown_fields makes
        // a config still sending it fail loudly rather than silently
        // ignoring a security-relevant key.
        let err = ClusterNatsConfig::parse(
            r#"{"servers": ["nats://nats:4222"], "node": {"id": "g1"}, "tls": {"verify_peer": true}}"#,
        )
        .unwrap_err();
        assert!(err.to_string().contains("verify_peer") || err.to_string().contains("unknown"));
    }

    #[test]
    fn http_server_url_rejected() {
        let err =
            ClusterNatsConfig::parse(r#"{"servers": ["http://nats:4222"], "node": {"id": "g1"}}"#)
                .unwrap_err();
        assert!(err.to_string().contains("scheme"));
    }

    #[test]
    fn empty_node_id_rejected() {
        let err = ClusterNatsConfig::parse(r#"{"servers": ["nats://x"], "node": {"id": ""}}"#)
            .unwrap_err();
        assert!(err.to_string().contains("node.id"));
    }

    #[test]
    fn peer_expiry_lt_heartbeat_rejected() {
        let err = ClusterNatsConfig::parse(
            r#"{"servers": ["nats://x"], "node": {"id": "g", "heartbeat_interval_sec": 30, "peer_expiry_sec": 10}}"#,
        )
        .unwrap_err();
        assert!(err.to_string().contains("peer_expiry_sec"));
    }

    #[test]
    fn renew_percent_out_of_range_rejected() {
        let err = ClusterNatsConfig::parse(
            r#"{"servers": ["nats://x"], "node": {"id": "g"}, "lease": {"default_ttl_sec": 30, "renew_before_expiry_percent": 100}}"#,
        )
        .unwrap_err();
        assert!(err.to_string().contains("renew_before_expiry_percent"));
    }

    #[test]
    fn unknown_field_rejected() {
        let err = ClusterNatsConfig::parse(
            r#"{"servers": ["nats://x"], "node": {"id": "g"}, "bogus": 1}"#,
        )
        .unwrap_err();
        assert!(err.to_string().contains("bogus"));
    }

    #[test]
    fn token_auth_parses() {
        let cfg = ClusterNatsConfig::parse(
            r#"{
                "servers": ["nats://x"],
                "node": {"id": "g"},
                "auth": {"method": "token", "token": "t"}
            }"#,
        )
        .unwrap();
        assert!(matches!(cfg.auth, Some(AuthConfig::Token { .. })));
    }

    #[test]
    fn credentials_file_auth_parses() {
        let cfg = ClusterNatsConfig::parse(
            r#"{
                "servers": ["nats://x"],
                "node": {"id": "g"},
                "auth": {"method": "credentials_file", "path": "/etc/nats.creds"}
            }"#,
        )
        .unwrap();
        assert!(matches!(cfg.auth, Some(AuthConfig::CredentialsFile { .. })));
    }

    /// A syntactically complete NATS credentials-file blob. The seed is
    /// the throwaway one async-nats' own tests use.
    fn creds_blob() -> String {
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

    fn parse_with_creds(creds: &str) -> Result<ClusterNatsConfig, ConfigError> {
        ClusterNatsConfig::parse(
            &serde_json::json!({
                "servers": ["tls://nats:4222"],
                "node": {"id": "g1"},
                "auth": {"method": "credentials", "creds": creds}
            })
            .to_string(),
        )
    }

    #[test]
    fn inline_credentials_auth_parses() {
        let cfg = parse_with_creds(&creds_blob()).unwrap();
        match cfg.auth {
            Some(AuthConfig::Credentials { ref creds }) => assert_eq!(*creds, creds_blob()),
            other => panic!("expected inline credentials auth, got {other:?}"),
        }
    }

    #[test]
    fn inline_credentials_missing_jwt_section_rejected() {
        let no_jwt = creds_blob().replace("NATS USER JWT", "SOMETHING ELSE");
        let err = parse_with_creds(&no_jwt).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("auth.creds"), "{msg}");
        assert!(msg.contains("-----BEGIN NATS USER JWT-----"), "{msg}");
    }

    #[test]
    fn inline_credentials_missing_seed_section_rejected() {
        // Half-pasted value: JWT block only.
        let jwt_only =
            "-----BEGIN NATS USER JWT-----\neyJhIjoiYiJ9\n------END NATS USER JWT------\n";
        let err = parse_with_creds(jwt_only).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("auth.creds"), "{msg}");
        assert!(msg.contains("-----BEGIN USER NKEY SEED-----"), "{msg}");
        // The credential itself must never appear in the error.
        assert!(
            !msg.contains("eyJhIjoiYiJ9"),
            "creds echoed in error: {msg}"
        );
    }

    #[test]
    fn inline_credentials_empty_value_rejected() {
        // An unset env var substitutes to an empty string — the shape
        // check must catch it rather than deferring to connect time.
        assert!(parse_with_creds("").is_err());
    }

    #[test]
    fn auth_debug_never_renders_secret_material() {
        let dbg = format!("{:?}", parse_with_creds(&creds_blob()).unwrap());
        assert!(!dbg.contains("SUACH75"), "seed leaked in Debug: {dbg}");
        assert!(!dbg.contains("eyJ0eXAi"), "jwt leaked in Debug: {dbg}");

        let cfg = ClusterNatsConfig::parse(
            r#"{
                "servers": ["nats://x"],
                "node": {"id": "g"},
                "auth": {"method": "token", "token": "hunter2"}
            }"#,
        )
        .unwrap();
        assert!(
            !format!("{cfg:?}").contains("hunter2"),
            "token leaked in Debug"
        );

        let cfg = ClusterNatsConfig::parse(
            r#"{
                "servers": ["nats://x"],
                "node": {"id": "g"},
                "auth": {"method": "user_password", "user": "svc", "password": "hunter2"}
            }"#,
        )
        .unwrap();
        let dbg = format!("{cfg:?}");
        assert!(!dbg.contains("hunter2"), "password leaked in Debug: {dbg}");
        assert!(
            dbg.contains("svc"),
            "non-secret user hidden in Debug: {dbg}"
        );
    }

    #[test]
    fn inline_pem_detection_both_ways() {
        assert!(is_inline_pem("-----BEGIN CERTIFICATE-----\nMIIB\n"));
        // Leading whitespace (a YAML block scalar keeps its newline).
        assert!(is_inline_pem("\n-----BEGIN CERTIFICATE-----\nMIIB\n"));
        assert!(!is_inline_pem("/etc/mcpg/certs/nats-ca.pem"));
        assert!(!is_inline_pem("relative/ca.pem"));
    }

    #[test]
    fn tls_ca_cert_accepts_inline_pem_and_path_verbatim() {
        for value in [
            "-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----\n",
            "/etc/mcpg/certs/nats-ca.pem",
        ] {
            let cfg = ClusterNatsConfig::parse(
                &serde_json::json!({
                    "servers": ["tls://nats:4222"],
                    "node": {"id": "g1"},
                    "tls": {"ca_cert": value}
                })
                .to_string(),
            )
            .unwrap();
            assert_eq!(cfg.tls.unwrap().ca_cert.as_deref(), Some(value));
        }
    }

    #[test]
    fn replicas_zero_rejected() {
        let err = ClusterNatsConfig::parse(
            r#"{"servers": ["nats://x"], "node": {"id": "g"}, "jetstream": {"replicas": 0}}"#,
        )
        .unwrap_err();
        assert!(err.to_string().contains("replicas"));
    }

    #[test]
    fn storage_defaults_to_file_for_production_durability() {
        let cfg = ClusterNatsConfig::parse(minimal()).unwrap();
        assert_eq!(cfg.jetstream.storage, KvStorage::File);
    }

    #[test]
    fn storage_memory_round_trips() {
        let cfg = ClusterNatsConfig::parse(
            r#"{"servers": ["nats://x"], "node": {"id": "g"}, "jetstream": {"storage": "memory"}}"#,
        )
        .unwrap();
        assert_eq!(cfg.jetstream.storage, KvStorage::Memory);
    }

    #[test]
    fn storage_unknown_value_rejected() {
        let err = ClusterNatsConfig::parse(
            r#"{"servers": ["nats://x"], "node": {"id": "g"}, "jetstream": {"storage": "tape"}}"#,
        )
        .unwrap_err();
        // serde renders this as "unknown variant `tape`, expected
        // one of `memory`, `file`". The exact wording is upstream
        // serde's; we only assert it names the bogus value so a
        // wording change still passes.
        let msg = err.to_string();
        assert!(
            msg.contains("tape") || msg.contains("variant"),
            "unknown-storage error should reference the bad value or variant; got: {msg}"
        );
    }
}
