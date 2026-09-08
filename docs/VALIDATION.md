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


## Release acceptance additions

The next source revision adds five real TLS certificate tests (trusted private
CA, untrusted CA, hostname mismatch, expiry and invalid configuration) and
`tests/acceptance/run.py` for bounded Rust/JS WSS messaging with three transport
cuts. Receipt files are produced only after checks and owned-process cleanup
succeed; CI retains exact-SHA receipts as artifacts.

The initial sustained run against server `132e46209d98fa0425cc0f88e7a97080cdad044d`
failed because queued server WebSocket payloads referenced a reused read buffer.
A deterministic two-read regression reproduced corruption. The acceptance
harness therefore pins the server fix `27a39f15bf163b433f417b78ab6bfc6e589585e5`
([server PR #901](https://github.com/WuKongIM/WuKongIM/pull/901)); do not attribute
successful recovery on that fix to the older server revision. Five certificate
tests and all 22 original Rust tests pass, as does `cargo publish --dry-run`.
