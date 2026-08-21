# P2P manager embed

Use `cmd/nats-p2p-server` with a combined config: standard NATS options plus a top-level `p2p { ... }` block stripped before `server.ProcessConfigFile`.

When disconnect release needs `$SYS.ACCOUNT.*.DISCONNECT`, set `system_account` in the NATS section and `sys_username` / `sys_password` / `agent_*` in `p2p {}`.

## Agent identity and wiring

- Agents must set the `Nats-P2P-Name` header to the same value as their NATS CONNECT `name` (the node key). REGISTER rejects a header/body mismatch.
- Production deployments should set `sys_username` / `sys_password` so disconnect release works and cluster coordination (`$P2P.MGR.*`) stays on the system account instead of the app/agent account.
- `$P2P.REGISTER` is handled with a conn-local `Subscribe` on each manager (not a queue group), so occupancy stays on the node that holds the named connection.
