# raypx2 V2 Integration Documentation Design

## Goal

Rewrite `docs/raypx2-integrate.md` as a self-contained Chinese integration guide for the current P2P V2 implementation. Remove the obsolete V1 protocol entirely, including its subjects, message shapes, state assumptions, and compatibility guidance.

## Scope

Only `docs/raypx2-integrate.md` is part of the user-facing documentation change. The implementation plan and this design record may live under `docs/superpowers/` as required by the project workflow. No Go source, tests, configuration examples, or other project documentation will be changed.

The implementation in `p2p/`, the wrapper in `cmd/nats-p2p-server`, its tests, and the current `p2p/README.md` are the sources of truth. Where narrative documentation disagrees with code or tests, the code and tests win.

## Audience and Style

The guide targets raypx2 client developers and operators deploying `nats-p2p-server`. It remains written in Chinese, keeps concrete subject and JSON examples, and separates client obligations from coordinator internals. It must be usable without first reading `p2p/README.md`.

## Document Structure

The rewritten guide will cover:

1. Architecture and process startup, including the always-on V2 Coordinator and the separation between NATS signaling and the QUIC/UDP data plane.
2. Identity and authorization, including `CONNECT name = node_key`, registration IDs, Auth Callout behavior, and node-scoped V2 permissions.
3. V2 protocol foundations: envelope fields, protocol version, identifiers, subjects, payload limits, and error envelopes.
4. The normal lifecycle: `REGISTER`, `SESSION.CREATE`, `BIND`/`PREPARE`, `READY`/`START`, and `SIGNAL.SEND`/`SIGNAL.ACK`.
5. Session and connection lifecycle commands, including `OPEN`, `RESTART`, `RESUME`, `REJECT`, and `CLOSE` where supported by the code.
6. Reliability and cluster semantics: queue coordination, owner/replica state, synchronization, retry, deduplication, timeouts, disconnect grace, and owner loss.
7. Limits and failure behavior, including the 128-connections-per-session limit, `busy` retry hints, coordinator unavailability, and the complete V2 error-code set.
8. Configuration and operations: wrapper startup, STUN/TURN validation, system-account credentials, ping defaults, and the V2 STATSZ endpoint and fields.
9. A raypx2 integration checklist covering required subscriptions, request idempotency, reconnect registration, signal sequencing, acknowledgements, and secret handling.

## Content Rules

- Use only `$P2P.V2.*` external protocol subjects and the current internal V2 subjects where operationally relevant.
- Do not mention the removed `$P2P.REGISTER`, `$P2P.CREATE`, `$P2P.UNREGISTER`, `$P2P.NODE.*`, or `$P2P.ICE.*` protocol.
- Do not include a V1 migration or compatibility section.
- Do not copy embedded usernames, passwords, issuer seeds, TURN secrets, or other sensitive constants from source code.
- Mark coordinator-only subjects and system-account requirements so agents do not attempt to use them.
- Keep examples consistent with the actual JSON decoder and encoder behavior, including required UUIDs, registration IDs, revisions, epochs, sequence numbers, and retry fields.
- Describe defaults and limits only when verified in code or tests.

## Verification

After the rewrite:

- Compare every documented subject, frame kind, state, error code, limit, timeout, and metric against its defining Go code and focused tests.
- Search the rewritten integration guide for obsolete V1 subjects and V1-specific terminology; none may remain.
- Check all Markdown headings, tables, and fenced examples for structural correctness.
- Run the focused P2P and wrapper test suites to ensure the referenced behavior still matches executable tests.
- Review the final diff to confirm no file outside the approved documentation scope was unintentionally changed.
