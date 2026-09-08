# WuKongEasySDK-Rust

[中文](README_zh.md)

Lightweight native async Rust SDK for WuKongIM, based on the WebSocket JSON-RPC
contract of [WuKongEasySDK-JS 2.0.4](https://github.com/WuKongIM/WuKongEasySDK-JS/tree/9c03c98c725982fac224cd1d3b52456eae983975).
It provides authenticated connections, online person/group messaging, automatic
RECVACK, heartbeat, bounded reconnection, and custom event notifications.

Rust **1.86+** with Tokio. Package: `wukong-easy-sdk`; import: `wukong_easy_sdk`.
Native TCP/TLS transports are supported; browser/WASM is outside this version.
WSS uses rustls and WebPKI roots with certificate verification enabled.
For private PKI, add DER roots through `Options.additional_root_certificates`;
public roots remain trusted, and hostname/expiry checks stay enabled. This accepts
at most 16 certificates of 64 KiB each and does not accept private keys.

## Install

Install the exact initial release from crates.io and commit Cargo.lock:

```toml
[dependencies]
wukong-easy-sdk = "=0.1.0"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
serde_json = "1"
```

For a sibling checkout during development:

```toml
wukong-easy-sdk = { path = "../WuKongEasySDK-Rust" }
```

## Connect and send

A trusted backend supplies the WebSocket URL, user ID and token. Each identity
owns a separate client; cloned handles share the same socket. The default device
category is **PC/Desktop `2`** (APP `0`, WEB `1`); backend token registration must
use the same category. `Auth::new` generates one device ID retained on reconnect;
set `auth.device_id` yourself to retain the device identity across process starts.

```rust,no_run
use serde_json::json;
use wukong_easy_sdk::{Auth, ChannelType, Client, Event, Options};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::new(
        std::env::var("WK_WS_URL")?,
        Auth::new(std::env::var("WK_UID")?, std::env::var("WK_TOKEN")?),
        Options::default(),
    )?;
    let mut events = client.subscribe(); // Subscribe before connecting.
    let listener = tokio::spawn(async move {
        loop {
            match events.recv().await {
                Ok(Event::Message(message)) => {
                    // Render/store message.payload in your application; avoid logging it.
                    let _ = &message.payload;
                }
                Ok(Event::CustomEvent(event)) => { let _ = &event.data; }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                    // Events were lost. Reconcile through your application's backend.
                    break;
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                _ => {}
            }
        }
    });
    let result = async {
        client.connect().await?;
        client.send("bob", ChannelType::Person, json!({"type":1,"content":"Hello 🦀"})).await?;
        Ok::<_, wukong_easy_sdk::Error>(())
    }.await;
    client.destroy().await; // Also runs when connect/send fails.
    listener.abort();
    let _ = listener.await;
    result?;
    Ok(())
}
```

The snippet sends once and exits. [examples/chat.rs](examples/chat.rs) keeps both
clients online, reads terminal input and handles Ctrl-C. After both users are
connected, type messages in either terminal:

```bash
# Terminal A; use backend-issued development credentials, device_flag=2.
WK_WS_URL=ws://127.0.0.1:5200 WK_UID=alice WK_TOKEN=alice-token \
  WK_PEER_UID=bob cargo run --locked --example chat
# Terminal B
WK_WS_URL=ws://127.0.0.1:5200 WK_UID=bob WK_TOKEN=bob-token \
  WK_PEER_UID=alice cargo run --locked --example chat
```

The example reports receipt without logging message bodies. Type `/quit` to
close. Use the payload in your own UI when you need to display content.

## API and lifecycle

| API | Contract |
| --- | --- |
| `Client::new(url, auth, options)` | Validates configuration without spawning tasks; errors contain no credentials |
| `subscribe()` | Bounded `broadcast::Receiver<Event>`; drop it to unsubscribe |
| `connect().await` | Returns `ConnectResult` after authentication; concurrent calls join the current attempt |
| `is_connected()` | Snapshot of authenticated transport state, not a delivery guarantee |
| `send(channel, type, payload).await` | Accepts a JSON object or array; returns typed `SendResult` |
| `send_with_options(..., SendOptions)` | Explicit `client_msg_no`, headers, setting and topic |
| `disconnect().await` | Cancels socket/auth/reconnect and awaits cleanup; allows later `connect` |
| `destroy().await` | Permanently closes every clone; later operations return `Destroyed` |
| Drop last `Client` | Cancels its worker; use explicit shutdown when completion must be awaited |

Events: `Connect`, `Disconnect`, `Message`, `SendAck`, `Reconnecting`,
`CustomEvent`, `Error`. Message/custom event bodies use `Arc` across subscribers.
The SDK emits no application logs. `Auth` Debug and SDK errors redact credentials,
URLs, raw frames, server error text and payloads. Full events contain application
data and must not be logged wholesale.

Initial connection failure returns an error and requires an explicit retry.
An established connection's transport failure or missing heartbeat triggers up to
five retries with exponential backoff (1, 2, 4, 8, 16 seconds plus up to 25% jitter,
capped at 30 seconds). Authentication rejection and server `disconnect` are
terminal. Manual disconnect/destroy cancels authentication, I/O and backoff.
Cancelling a `connect` future alone leaves the shared attempt active.

Sends are never replayed automatically. Timeout, cancellation or connection loss
can leave server acceptance unknown; keep `SendOptions.client_msg_no` when your
application chooses to retry using the server's idempotency contract. A server
SENDACK is not a recipient read receipt. RECVACK is automatic transport receipt,
not proof that a listener handled or persisted the event.

## Bounds and protocol

| Option | Default |
| --- | --- |
| `connect_timeout` / `request_timeout` / `write_timeout` | 5 s / 15 s / 5 s |
| `ping_interval` / `pong_timeout` | 25 s / 10 s |
| `max_reconnect_attempts` | 5; `0` disables automatic retries |
| `reconnect_delay` / `max_reconnect_delay` | 1 s / 30 s |
| `max_in_flight` | 256 across queued and pending sends; excess returns `Backpressure` |
| `event_capacity` | 256; slow listeners get `RecvError::Lagged` |
| `max_message_size` | 1 MiB per complete incoming/outgoing JSON-RPC message |
| `additional_root_certificates` | Empty; optional extra DER CA roots |

Durations must be positive and at most one day; reconnect cap must be at least
the initial delay; retries are at most 100; queue counts are 1–65,536; message
limits are 256 bytes–64 MiB. Size checks apply to the encoded envelope, including
Base64 overhead. The application still owns the memory of payload values passed
to `send`. A full/absent observer does not stop RECVACK: EasySDK has no durable
inbox; applications needing reliable catch-up must provide their own persistence
and synchronization.

Requests use JSON-RPC 2.0, string IDs, camelCase metadata and Base64 UTF-8 JSON
payloads. Receive supports object payloads and decodable Base64 JSON, retaining
other strings as in JS. Response metadata accepts camelCase and snake_case,
including both at once; application payload keys remain unchanged. Message IDs
stay strings; sequence numbers use `u64`. Custom-event JSON text is decoded;
other data stays unchanged. Custom-event support is a protocol capability, not
a claim about which events a particular product deployment emits.

`Header.red_dot` defaults to true for SEND, but explicit false is preserved.
This intentionally differs from JS 2.0.4 forcing it to true. Group membership,
history, conversations, unread state, offline recovery, push, subscriptions,
batches and general-purpose RPC are not implemented here. Supported Channel
constants do not imply every type is enabled on your server.

## Development and live verification

```bash
cargo fmt --all -- --check
cargo test --locked
cargo clippy --locked --all-targets -- -D warnings
cargo build --locked --examples
cargo doc --locked --no-deps
cargo package --locked
```

Tests use bounded loopback peers for correlation, mixed field profiles, Unicode,
large IDs, RECVACK, events, timeout, backpressure, reconnect cancellation, last
handle drop, lag and terminal failures. CI runs Rust 1.86 and stable on Linux,
plus stable on Windows/macOS.

To reproduce the real-server Rust/Rust smoke after backend registration:

```bash
WK_WS_URL=ws://127.0.0.1:5200 WK_UID=alice WK_TOKEN=alice-token \
  WK_PEER_UID=bob WK_PEER_TOKEN=bob-token cargo run --locked --example roundtrip
```

For JS interoperability install `easyjssdk@2.0.4` in a separate directory, start
[tests/interop.mjs](tests/interop.mjs) with `EASYSDK_JS_DIR`, `WK_WS_URL`,
`WK_UID=bob`, `WK_TOKEN` (registered as WEB `1`), wait for `READY`, and run
`roundtrip` with Alice's PC `2` credentials **without** `WK_PEER_TOKEN`. Stop the
JS peer with SIGTERM afterwards. It has a 30-second deadline and echoes two
messages. [docs/VALIDATION.md](docs/VALIDATION.md) records exact verified versions
and limits. Production use must validate its own WSS/proxy, credential rotation,
network, OS and load conditions.


### Automated real-server acceptance

CI builds a pinned WuKongIM single-node cluster with 256 Hash Slots and Token
validation, runs Rust/Rust messaging and explicit invalid-Token rejection, then
runs Rust against npm `easyjssdk@2.0.4` through a TLS proxy. During sustained
Unicode exchanges it cuts the Rust transport three times and requires automatic
reconnect plus successful messaging after every recovery. Certificates, identity
setup, listeners and processes belong to the test harness and are cleaned up.

```bash
git clone https://github.com/WuKongIM/WuKongIM.git test-server
git -C test-server checkout 27a39f15bf163b433f417b78ab6bfc6e589585e5
python3 tests/acceptance/run.py --server-source test-server --seconds 120
```

Prerequisites: Rust 1.86+, Go 1.25.11, Node 22.12+ with npm, Python 3.11+ and
OpenSSL. The default CI run lasts 120 seconds of WSS messaging; a manual
`Real server acceptance` run accepts 600 seconds. A successful run retains
`.acceptance/receipt.json`, including exact source revisions, message counts,
interruptions and cleanup. This bounded recovery check is not a capacity or
multi-day soak claim. Five separate TLS tests cover trusted roots, unknown CA,
wrong hostname, expired certificates and invalid configuration.


## Published package acceptance

The real-server harness can build its probes as an independent consumer of
`wukong-easy-sdk = "=0.1.0"`. Registry mode starts with an empty Cargo cache,
verifies the public archive checksum and source identity, and uses no SDK path
or Git dependency. Both modes retain separate receipts in CI.

```sh
python3 tests/acceptance/run.py --server-source test-server --distribution registry --seconds 120 --output .acceptance/registry.json
```

The server checkout must be the documented pinned revision. A registry receipt
names the released package source separately from the harness revision.


### Group membership and permissions acceptance

Both source and registry runs also connect four Rust identities over verified
WSS to two groups. The trusted harness creates the groups and changes membership
through Product HTTP; the SDK clients only use the message gateway. Ten phases
check member fanout, group isolation, outsider rejection (reason 3), add/remove/
re-add, denylist rejection (reason 4) and removal, and membership after all four
clients automatically reconnect from a transport cut. Each delivered message
must match its SENDACK ID/sequence, sender, channel and Unicode payload, without
duplicates or observer lag. Excluded clients are observed for at least 500 ms
per phase. The nested `group` receipt records every phase and client cleanup.
This four-client check does not establish large-group capacity, offline catch-up
or cross-node routing behavior.


### Weak-network and resource acceptance

Every source/registry run also repeats two-client WSS lifecycles for at least
30 seconds and four cycles. It injects deterministic 20/40/60 ms per-chunk delay,
pauses return traffic to prove peer delivery despite SEND timeout, verifies
`Backpressure` at two pending requests and admission recovery, and requires
`Lagged` for a deliberately slow 16-event observer while an active observer
checks all deliveries. Alternating blackholes and transport aborts must recover
automatically, with no old SEND observed replaying. Each cycle destroys both
clients and requires both proxies to drain all streams/tasks.

```sh
python3 tests/acceptance/run.py --server-source test-server --distribution registry --seconds 120 --network-seconds 1800 --output .acceptance/network-1800s.json
```

The explicit long mode runs the weak-network loop for at least 30 minutes,
plus build time and the existing person/group checks. The manual Workflow input
`network_seconds=1800` selects it; ordinary CI stays short. Resource measurement
supports Linux and macOS and samples the Rust probe's RSS and numeric file
descriptors after each cycle. After three warmup cycles, fixed allowances are
64 MiB RSS and eight descriptors above baseline; proxy streams/tasks must be
zero. The `network` receipt retains every cycle, fault/recovery timing, resource
sample and cleanup result. A bounded proxy-side WebSocket audit requires exactly
58 outbound SEND requests per cycle, matching 58 verified deliveries; server
deduplication therefore cannot hide an extra retransmission. The audit retains
counts only and fails on unsupported framing. Probe settings (800 ms SEND timeout, 3 s pong timeout,
2 pending SENDs, 16 events) are distinct from SDK defaults. The replay observation
window is 150 ms per quiet check, with later phases also rejecting old payloads.
These conservative finite checks do not prove production capacity, multi-day
stability, every network failure mode or absence of every resource leak.

### Three-node cluster acceptance

Run the independent public-package probe against the exact clean server checkout:

```bash
RUSTUP_TOOLCHAIN=1.86.0 python3 tests/acceptance/cluster.py \
  --server-source ../test-server --distribution registry --seconds 600
```

This harness pins server `f041174a042b4a96179218571e06c04bb64cf1ca`, which includes
[the cross-node group membership cache fix](https://github.com/WuKongIM/WuKongIM/pull/920).
The older single-node server pin does not establish correct cross-node member
removal: the three-node probe reproduced a removed member receiving a new group
message even after a five-second wait. The merged server fix refreshes mutable metadata at the Slot authority and uses
bounded authoritative subscriber pages; SDK 0.1.0 is unchanged.

Three isolated local server processes use 256 hash slots, 12 logical slots and
three Slot replicas. Four Rust clients authenticate through verified private-CA
WSS on nodes `1, 2, 3, 2`. The probe verifies all six directed person paths between
the first three clients, group fanout/isolation, nonmember and denylist rejection,
member removal/re-add, and permissions after ingress node 1 is killed and restarted
with the same address/data. A withheld SENDACK produces `Timeout` while its peer
receives the message. Independent wire SEND counts must equal all application
attempts, including rejected and uncertain sends, before and after reconnection.
No application retry is performed and server deduplication cannot hide replay.

CONNECT recovery is separate from route recovery. Exploratory immediate sends
observed SENDACK without delivery while recipients were absent from the online
route view after Slot authority changes. The acceptance therefore requires all
four users online through every API ingress for two 25-second heartbeat intervals
before asserting steady-state delivery. This 50-second observation is bounded by
a 100-second gate; receipts separate socket reconnection from gate completion.
SENDACK still means server acceptance, not recipient delivery.

[Dedicated CI](.github/workflows/cluster.yml) runs a 60-second workload for source
and exact registry distributions; manual runs select 600 seconds. The initial
fault/permission suite precedes that workload; a second crash and recovery gate
occur at its midpoint. Timed loops include recovery and finish the current phase,
so a short run can exceed 60 seconds. Every successful run requires client
shutdown, an empty online-status response and zero owned processes/proxy streams/
tasks. Failed receipts retain bounded routing/delivery diagnostics. Test timers:
SEND 3 s, connect 2 s, PONG 10 s, heartbeat 25 s, 100 retries with 100–500 ms backoff.
These differ from SDK defaults. Exclusions are observed for at least 500 ms per phase.

This finite test does not establish alternate-address failover, uninterrupted
availability during failures, selected Channel leader transfer, network partition
recovery, offline catch-up, large-group capacity or multi-day stability.
