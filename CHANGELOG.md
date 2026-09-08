# Changelog

## Unreleased

- Keep package paths anchored to exclude nested third-party assets; bind acceptance receipts to unchanged SDK revisions.

- Add optional private DER root certificates while retaining hostname and expiry verification.
- Add WSS certificate tests and reproducible Rust/JS real-server acceptance with three transport interruptions and bounded sustained messaging.

- Initial 0.1.0 Rust implementation, based on WuKongEasySDK-JS 2.0.4.
- Add native Tokio WS/WSS connections, JSON-RPC authentication, person/group SEND,
  automatic RECVACK, custom events, heartbeat and bounded reconnect.
- Add typed asynchronous API, shared client lifecycle, bounded requests/events,
  payload/field compatibility and safe errors with no automatic logs.
- Add bilingual guides, terminal examples and repeatable JS interoperability smoke.
- Source distribution only; no crates.io release has been published.
