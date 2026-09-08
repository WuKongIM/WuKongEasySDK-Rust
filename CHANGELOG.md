# Changelog

## Unreleased

- Add released-package three-node WSS acceptance for cross-node person/group delivery, membership permissions, uncertain SENDs and same-endpoint ingress crash recovery; distinguish socket recovery from volatile presence route recovery.

- Verify weak-network SEND ambiguity, admission backpressure, slow-observer lag and repeated client cleanup with public-package resource samples; add an explicit 30-minute acceptance mode.

- Add real-server group acceptance for the source and exact released crate: member fanout, isolation, membership changes, explicit permission rejection and four-client WSS reconnect.

- Verify the published 0.1.0 crate with an empty-cache consumer, exact archive/source identity, real-server Rust/JS WSS messaging and three transport cuts; retain registry and source receipts separately.

## 0.1.0 — 2026-09-08

- Keep package paths anchored to exclude nested third-party assets; bind acceptance receipts to unchanged SDK revisions.

- Add optional private DER root certificates while retaining hostname and expiry verification.
- Add WSS certificate tests and reproducible Rust/JS real-server acceptance with three transport interruptions and bounded sustained messaging.

- Initial 0.1.0 Rust implementation, based on WuKongEasySDK-JS 2.0.4.
- Add native Tokio WS/WSS connections, JSON-RPC authentication, person/group SEND,
  automatic RECVACK, custom events, heartbeat and bounded reconnect.
- Add typed asynchronous API, shared client lifecycle, bounded requests/events,
  payload/field compatibility and safe errors with no automatic logs.
- Add bilingual guides, terminal examples and repeatable JS interoperability smoke.
- Publish the initial `wukong-easy-sdk` crate with Rust 1.86+ support.
