# P2P manager embed

Use `cmd/nats-p2p-server` with a combined config: standard NATS options plus a top-level `p2p { ... }` block stripped before `server.ProcessConfigFile`.

The wrapper always starts the V2 Coordinator (`StartManager`) after the embedded server is ready. There is no V1 REGISTER/CREATE/UNREGISTER/ICE path and no process-level session cap.

When disconnect release needs `$SYS.ACCOUNT.*.DISCONNECT`, set `system_account` in the NATS section and `sys_username` / `sys_password` / `agent_*` in `p2p {}`.

## V2 subjects

Agents authenticate with CONNECT `name` = node key. Auth Callout issues only that node's V2 subjects:

| Direction | Subject |
| --- | --- |
| Command (publish) | `$P2P.V2.CMD.<node-key>.>` |
| Event (subscribe) | `$P2P.V2.EVENT.<node-key>.>` |
| Request/reply | `_INBOX.>` |

Coordinator subscriptions (not granted to agents):

- Register: `$P2P.V2.CMD.*.REGISTER.<own-server-id>` (conn-local, not a queue group)
- Session/connection/signal: `$P2P.V2.CMD.*.SESSION.CREATE` / `SESSION.COMMAND` / `CONNECTION.COMMAND` / `SIGNAL.SEND` / `SIGNAL.ACK` on queue group `$P2P.V2.COORDINATORS`
- Internal: `$P2P.V2.MGR.SYNC` / `SNAPSHOT` / `OWNERLOST` / `$P2P.V2.MGR.<server-id>.COMMAND`
- Cluster liveness: `$P2P.MGR.BEAT` (and leftover `$P2P.MGR.PREPARE` / `COMMIT` / `RELEASE` for peer tracking)

Agents never receive `$P2P.V2.MGR.>` or another node's CMD/EVENT.

## State machine

Session: `allocated` → `preparing` → `active` → `draining` / `closed` / `failed`.

Connection: `preparing` → `active` → `restarting` / `closed`.

Typical path:

1. Both nodes `REGISTER` on `$P2P.V2.CMD.<node>.REGISTER.<server-id>` (connz must be exactly one named connection).
2. Client `SESSION.CREATE` → `ALLOCATED` (`conn-0`, epoch 1).
3. Client `SESSION.COMMAND` BIND → both sides get `PREPARE` (STUN, optional TURN, `feature_bits=1`, setup deadline).
4. Both `CONNECTION.COMMAND` READY → `START`.
5. ICE goes through `SIGNAL.SEND` / `SIGNAL.ACK` (no shared ICE subject). OPEN/RESTART allocate extra connections up to **128 per session**.

## Always-on Coordinator

`p2p {}` may be empty. The wrapper still `ValidateConfig` + `StartManager`. Startup initializes V2 Store, command subscriptions, signal retry worker, and expiry worker. Catch-up must finish before the node joins `$P2P.V2.COORDINATORS`.

## Limits and failure semantics

- Occupancy is Store-backed: a node key is registered, disconnected (grace), then expired. No process-wide session or connection cap.
- Per-session connection limit is 128 (`connection_limit` + `limit: 128`).
- Queue saturation and cluster sync timeout reply `busy` + `retry_after_ms` in `[100, 1000]`.
- Unreachable owner replies `coordinator_unavailable` (no takeover).
- Setup timeout publishes `ERROR` (`setup_timeout`) without TURN passwords.
- Owner leave: negotiating sessions get rebuild `ERROR`; ACTIVE sessions are not force-CLOSED.

## Auth Callout

To delegate CONNECT authentication, add a standard NATS `authorization { auth_callout { ... } }` block (not a top-level `auth {}`).

Required:

- `issuer`: public account nkey that matches the embedded seed in `p2p/auth_callout.go` (`IssuerPublic()`).
- `auth_users`: in-process clients only (`auth-internal`, and `p2p-internal` / `sys` if used).
- `users`: passwords for those internal names. `p2p { agent_username / agent_password }` must equal `p2p-internal`.
- Do **not** add extra `permissions` on `auth-internal`. The server replies on `$SYS._INBOX.*` (`newRespInbox`), not `_INBOX.>`. Restricting publish to `_INBOX.>` (or subscribe-only `$SYS.REQ.USER.AUTH`) drops the reply; every CONNECT then fails with `Authorization Violation` after `authorization.timeout`. Leave both internal users unrestricted, matching `p2p/auth_callout_test.go`.

The wrapper starts the in-process callout subscriber after `ReadyForConnections` and before `StartManager`. Agent username/password are temporary constants in `p2p/auth_callout.go`; do not put them in this README. Without `auth_callout`, behavior stays on the static user list.
