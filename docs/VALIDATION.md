# Initial Rust source verification — 2026-09-08

This record describes the code in this commit, not a crates.io release.

- Reference: WuKongEasySDK-JS 2.0.4, commit
  `9c03c98c725982fac224cd1d3b52456eae983975`.
- Server: WuKongIM `132e46209d98fa0425cc0f88e7a97080cdad044d`.
- Runtime: macOS, Rust/cargo 1.86.0; locked Cargo dependencies.
- Topology: real `cmd/wukongim` process, single-node cluster, 256 Hash Slots,
  10 initial logical Slots, loopback-only listeners, `gateway.token_auth_on=true`.
- Identity setup: trusted loopback `/user/token`; Rust device category PC `2`,
  JavaScript category WEB `1`. Test identities and payloads were synthetic.

## Results

- `cargo test --locked`: 4 protocol unit tests and 18 bounded socket integration
  tests, including result correlation, Unicode, mixed aliases, u64 sequence
  preservation, acknowledgement, custom events, authentication failure,
  handshake timeout, request timeout, backpressure, heartbeat, retry exhaustion,
  reconnect cancellation, shutdown, oversized input/output and lag detection.
- `cargo fmt`, Clippy with warnings denied, both example builds, API docs and
  `cargo package` verification passed.
- `examples/roundtrip.rs`: Rust/Rust exchange passed twice, with explicit
  disconnect/reconnect between rounds and multiple JSON-RPC heartbeat periods.
- The same Rust example with `tests/interop.mjs` loaded from the actual npm
  `easyjssdk@2.0.4` package passed both directions and both reconnect rounds.
- Incorrect Token authentication was rejected by the real Product Gateway.
- Clients were destroyed and both owned server/JS processes were stopped.

Reproduce with the README's commands after starting the exact server revision
and registering credentials for the matching device categories. The SDK never
registers its own identity through Product HTTP.

This local run does not verify WSS/proxy deployment, physical devices, offline
recovery, large groups, capacity, soak duration or all OS/runtime combinations.
The CI matrix checks compilation/tests on Linux, Windows and macOS separately;
its actual workflow result is distinct from this local record. Custom-event
handling is verified against mock protocol notifications, not asserted as a
product-wide event-emission guarantee.
