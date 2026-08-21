# P2P manager embed

Use `cmd/nats-p2p-server` with a combined config: standard NATS options plus a top-level `p2p { ... }` block stripped before `server.ProcessConfigFile`.

When disconnect release needs `$SYS.ACCOUNT.*.DISCONNECT`, set `system_account` in the NATS section and `sys_username` / `sys_password` / `agent_*` in `p2p {}`.
