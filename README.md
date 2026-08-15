# NATS JetStream Cluster Coordinator — `dev.mcpg.cluster.nats`

> class `cluster` · `native` · package `mcpg-plugin-cluster-nats` · artifact `libmcpg_plugin_cluster_nats.so` · BUSL-1.1

The cluster coordinator an MCPG gateway fleet uses when NATS is already in the
stack. It maps everything a multi-replica gateway needs onto JetStream: KV
buckets hold leases, locks, fencing counters, and shared capability state;
subjects carry peer heartbeats and cross-replica notifications. It exposes all
four coordinator primitives — key/value store, pub/sub, lease, and watch — so
gateway capabilities inherit durable, replicated state without any further
wiring. Reach for it when you want one durable coordination backend and NATS is
the messaging system you already operate.

## What it does
- Advertises the `kv` and `bus` coordinator roles, so sessions, tasks,
  subscriptions, pipelines, delivery, and cancellation state is shared across
  replicas instead of living in each one's memory.
- Backs leadership and locks with JetStream KV: acquisition is an atomic
  `create` in the leases bucket, and renewal and release are compare-and-swap
  against the holder's known revision.
- Mints each fencing token by incrementing a CAS-guarded counter in the separate
  fencing bucket, so monotonicity is a property of the counter held in NATS
  rather than of any one replica's memory. Tokens are monotonic but not
  gap-free — a lost CAS race skips a value, so whatever consumes a token must
  compare with `>=`, never `==`.
- Renews held leases in the background, waking with
  `lease.renew_before_expiry_percent` of the TTL still to spare, so the default
  of 50 renews at the half-way point.
- Publishes and subscribes over NATS subjects rooted at `mcpg.notify.<topic>`,
  with an optional routing key as the trailing subject token so subscribers can
  filter server-side. A subscriber that names a group joins a NATS queue group
  and load-balances with its peers; an unnamed subscriber receives every
  message.
- Tracks peers by heartbeat on `mcpg.peers.heartbeat.<node id>`: a peer that has
  missed two heartbeat intervals is marked degraded, and one silent for
  `node.peer_expiry_sec` is evicted. Join, leave, and health-change events are
  streamed to watchers.
- Escapes gateway key names into the JetStream KV alphabet with a
  prefix-preserving encoding, so prefix listing still works for keys containing
  characters JetStream KV would reject.
- Connects lazily: the plugin registers without touching the broker, and a NATS
  outage at boot degrades individual operations rather than preventing startup.
- Declares the `network_outbound` capability; the gateway refuses to load the
  plugin unless the `plugins[]` entry grants it.

## Configuration
Selected by the dedicated top-level `cluster:` block through `cluster.kind:
nats`. The kind-specific fields are written **flat** under `cluster:` and flow
to the plugin's factory as JSON, replacing any `config:` block on the matching
`plugins[]` entry — so the `plugins[]` entry keeps the artifact location and the
`cluster:` block keeps the operational knobs. The cdylib must still be declared
in `plugins[]`; if `cluster.kind` names a coordinator with no matching entry,
the gateway fails fast at boot.

```yaml
cluster:
  kind: nats
  servers:
    - tls://nats-0.internal:4222
    - tls://nats-1.internal:4222
  node:
    id: ${env.HOSTNAME}
  auth:
    method: credentials_file
    path: /etc/mcpg/nats.creds
  tls:
    ca_cert: /etc/mcpg/certs/nats-ca.pem
    require_tls: true
  jetstream:
    replicas: 3
    storage: file
  lease:
    default_ttl_sec: 30
    renew_before_expiry_percent: 50

plugins:
  - id: dev.mcpg.cluster.nats
    class: cluster
    kind: native
    source:
      path: ./plugins/libmcpg_plugin_cluster_nats.so
      # or, platform-agnostic:
      # oci: ghcr.io/mcpg-dev/source-code/plugins/cluster-nats:protocol-1
    granted_capabilities:
      - network_outbound
```

| Field | Type | Default | Description |
|---|---|---|---|
| `servers` | string[] | — (required) | One or more NATS URLs; the client load-balances and reconnects across them. Accepted schemes: `nats://`, `tls://`, `nats+tls://`, `ws://`, `wss://`. |
| `node.id` | string | — (required) | Unique per replica. It is the trailing token of the heartbeat subject, so it must be a single NATS subject token — no `.`, `*`, `>`, whitespace, or control characters. |
| `node.address` | string | unset | Advertised address stored alongside the peer record. |
| `node.heartbeat_interval_sec` | integer | `10` | Heartbeat publish cadence, in seconds. Must be greater than zero. |
| `node.peer_expiry_sec` | integer | `30` | Seconds without a heartbeat before a peer is evicted and a leave event is emitted. Must exceed `node.heartbeat_interval_sec`. |
| `auth` | tagged object | unset | Selected by `method`: `token` (`token`), `user_password` (`user`, `password`), or `credentials_file` (`path`). |
| `tls.require_tls` | bool | `true` | TLS is required even when the whole `tls` block is omitted. |
| `tls.ca_cert` | string | system roots | PEM root certificate for a private CA. Server-certificate verification stays on. |
| `jetstream.leases_bucket` | string | `mcpg-leases` | KV bucket for leases and locks. Created on first connect if missing. |
| `jetstream.fencing_bucket` | string | `mcpg-fencing` | KV bucket for fencing-token counters. Created on first connect if missing. |
| `jetstream.notifications_stream` | string | `mcpg-notifications` | Stream name reserved for pub/sub fan-out. Validated as non-empty; the pub/sub path itself runs over core NATS subjects. |
| `jetstream.state_bucket` | string | `mcpg-state` | KV bucket for inherited capability state, kept separate from the coordinator's own buckets. |
| `jetstream.replicas` | integer | `1` | Replication factor for the KV buckets the plugin creates. Must be greater than zero; use `3` on a real NATS cluster. |
| `jetstream.storage` | `file` \| `memory` | `file` | `file` survives NATS restarts; `memory` is lighter but loses lease and capability state on any restart. |
| `jetstream.domain` | string | unset | JetStream domain, for leaf-node deployments. |
| `lease.default_ttl_sec` | integer | `30` | TTL used when the caller does not supply one. Must be greater than zero. |
| `lease.renew_before_expiry_percent` | integer | `50` | What share of the TTL must still remain when renewal fires; the renewal task sleeps through the rest of the TTL first. Must be within `1..=99`. |
| `connection.connect_timeout_ms` | integer | `5000` | Connect deadline, in milliseconds. Must be greater than zero. |
| `connection.operation_timeout_ms` | integer | `10000` | Per-operation deadline, in milliseconds. Must be greater than zero. |

Unknown fields are rejected in the top-level block and in each of the `node`,
`tls`, `jetstream`, `lease`, and `connection` blocks.

## Operations
These are the NATS resources the plugin touches — the set to grant when you
write subject and bucket permissions for the gateway's NATS user:

| Resource | Name | Purpose |
|---|---|---|
| KV bucket | `jetstream.leases_bucket` | Lease and lock records. |
| KV bucket | `jetstream.fencing_bucket` | Per-key fencing counters. |
| KV bucket | `jetstream.state_bucket` | Capability state inherited from the coordinator. |
| Subject | `mcpg.notify.<topic>.<routing key>` | Pub/sub. Always four tokens; an unfiltered subscriber matches the last one with `*`. |
| Subject | `mcpg.peers.heartbeat.<node id>` | Peer presence; the subscriber watches `mcpg.peers.heartbeat.*`. |

`_default_` is reserved as the routing-key token for a publish with no routing
key, so that subscriber wildcards keep a constant subject arity. Passing it
explicitly as a routing key is rejected.

## Security
- TLS is required by default. A plaintext link needs an explicit
  `tls: { require_tls: false }`, and the gateway additionally refuses to boot a
  non-`single_node` coordinator on a plaintext transport unless
  `cluster.allow_insecure_transport: true` is set — intended for local
  development and CI only. When TLS is negotiated the server certificate is
  always verified; there is no skip-verify knob.
- Prefer `credentials_file` auth and keep the `.creds` file out of the config
  artifact. A static `token` or password should come from the environment or a
  secret provider.
- Give each deployment sharing a NATS cluster its own bucket and stream names,
  and fence them with NATS subject permissions so deployments cannot read each
  other's coordination traffic.
- Coordinator-backed capability state can additionally be sealed at the
  application layer with `cluster.state_encryption_key_env`, which names the
  environment variable holding the key; keys and subjects stay cleartext for
  routing while values are encrypted.

## Build
`cdylib-export` is enabled by default, so the plain build already produces the
loadable artifact. Disable the default features when linking this crate as an
rlib path dependency alongside other plugins, so the workspace build does not
link two `mcpg_plugin_register` exports.

```bash
cargo build -p mcpg-plugin-cluster-nats --features cdylib-export --release   # → target/release/libmcpg_plugin_cluster_nats.so
```

## Testing
The unit suite is offline:

```bash
cargo test -p mcpg-plugin-cluster-nats --lib
```

The integration suites need a Docker daemon. They boot a JetStream-enabled NATS
container and run both this plugin's own tests and the shared coordinator
equivalence suite — the same suite every other coordinator runs, which is what
proves the backends behave identically:

```bash
cargo test -p mcpg-plugin-cluster-nats --features integration-tests
```

## Sign & load (production)
Sign the artifact, pin/verify via the entry's `signature:` block, and honour
revocations. See <https://mcpg.dev/docs/security/plugin-security>.

## See also
- <https://mcpg.dev/docs/self-hosting/clustering> — the coordinator model, the primitive-inheritance rules, and every backend's keys.
- <https://mcpg.dev/docs/plugins/plugins-and-protocol> — plugin classes, the ABI, and how the gateway loads them.
- `libs/plugins/cluster/consul`, `libs/plugins/cluster/etcd`, `libs/plugins/cluster/redis` — the sibling coordinators.
