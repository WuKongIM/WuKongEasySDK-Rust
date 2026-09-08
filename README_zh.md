# WuKongEasySDK-Rust

[English](README.md)

参考 [WuKongEasySDK-JS 2.0.4](https://github.com/WuKongIM/WuKongEasySDK-JS/tree/9c03c98c725982fac224cd1d3b52456eae983975) 实现的原生 Rust 异步轻量 SDK，提供 WebSocket JSON-RPC 连接、在线单聊/群聊、自动 RECVACK、心跳、有限自动重连和自定义事件。

需要 Rust **1.86+** 与 Tokio；包名 `wukong-easy-sdk`，导入名 `wukong_easy_sdk`。支持原生 TCP/TLS，WSS 默认使用 rustls 与 WebPKI 根证书校验；此版本不支持浏览器/WASM。

## 安装

从 crates.io 安装精确版本 `0.1.0`，并提交 Cargo.lock：

```toml
[dependencies]
wukong-easy-sdk = "=0.1.0"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
serde_json = "1"
```

本地开发也可以使用 `wukong-easy-sdk = { path = "../WuKongEasySDK-Rust" }`。

## 开始收发

业务后端提供 `uid`、短期 `token` 与 `websocketUrl`。每个身份创建一个 `Client`；`clone` 共享同一条连接。默认设备类别是 **PC/Desktop `2`**，APP 为 `0`，WEB 为 `1`；后端保存 Token 时必须匹配设备类别。`Auth::new` 生成一次设备 ID 并在重连时复用；需要跨进程保持设备身份时设置 `auth.device_id`。

```rust,no_run
use serde_json::json;
use wukong_easy_sdk::{Auth, ChannelType, Client, Options};

# async fn example() -> Result<(), Box<dyn std::error::Error>> {
let client = Client::new(
    std::env::var("WK_WS_URL")?,
    Auth::new(std::env::var("WK_UID")?, std::env::var("WK_TOKEN")?),
    Options::default(),
)?;
let mut events = client.subscribe(); // 在 connect 前订阅，在独立任务中持续消费。
let result = async {
    client.connect().await?;
    client.send("bob", ChannelType::Person, json!({"type":1,"content":"你好 🦀"})).await?;
    Ok::<_, wukong_easy_sdk::Error>(())
}.await;
client.destroy().await; // 失败时也执行清理。
drop(events);
result?;
# Ok(())
# }
```

完整接收循环见 [英文 README](README.md#connect-and-send)；[examples/chat.rs](examples/chat.rs) 包含终端输入、接收事件、退出及清理。为 Alice 与 Bob 在业务后端注册 PC `2` 开发身份后，在两个终端分别运行：

```bash
WK_WS_URL=ws://127.0.0.1:5200 WK_UID=alice WK_TOKEN=alice-token \
  WK_PEER_UID=bob cargo run --locked --example chat

WK_WS_URL=ws://127.0.0.1:5200 WK_UID=bob WK_TOKEN=bob-token \
  WK_PEER_UID=alice cargo run --locked --example chat
```

等两端连接成功后输入内容，`/quit` 或 Ctrl-C 退出。示例只打印收发状态；在自己的 UI 中显示 `message.payload`，避免把正文和 Token 写入日志。

## API 与生命周期

| API | 行为 |
| --- | --- |
| `Client::new` | 同步校验配置，不启动任务 |
| `subscribe()` | 有界 Tokio broadcast 接收器；drop 即取消订阅 |
| `connect().await` | 等待鉴权结果；并发调用共享当前连接尝试 |
| `is_connected()` | 当前鉴权连接状态快照 |
| `send(...).await` | 接受 JSON 对象/数组，返回服务端 `SendResult` |
| `send_with_options(...)` | 自定义 `client_msg_no`、Header、Setting、Topic |
| `disconnect().await` | 取消连接、鉴权和重连并等待退出；之后可再次连接 |
| `destroy().await` | 永久关闭当前客户端及其所有 clone |
| drop 最后一个 Client | 取消后台任务；需要等待完成时显式调用清理 API |

事件包括 `Connect`、`Disconnect`、`Message`、`SendAck`、`Reconnecting`、`CustomEvent` 和 `Error`。SDK 默认静默且不提供正文日志开关；错误和 Auth Debug 不包含 Token、URL、原始帧或服务端原文。完整事件中含有业务数据，不应直接打印。

首次连接失败返回错误，由应用决定是否再次调用 `connect`。已成功连接后的传输中断、心跳超时触发最多 5 次重连，延迟为 1/2/4/8/16 秒，加最多 25% 抖动，总延迟上限 30 秒。鉴权失败和服务端 `disconnect` 不重连。手动断开取消鉴权、I/O 和重连等待。仅取消 `connect` Future 不取消共享连接尝试。

SEND 默认 `red_dot=true`，尊重显式 `false`；这与 JS 2.0.4 强制设置为 true 的实现有意不同。发送不会自动重放；超时、取消或断线时，服务端是否已接受可能未知。业务决定重试时保留 `client_msg_no`，遵循服务端幂等规则。

SENDACK 不是对方已读回执；自动 RECVACK 不是业务已处理证明。群成员由业务后端准备。SDK 不提供本地数据库、离线同步、会话/未读、推送、订阅、批量或任意 RPC。

## 资源上限

连接/发送/写入超时默认 5/15/5 秒，心跳间隔/超时 25/10 秒；队列和未完成请求总数默认 256，超出返回 `Backpressure`；事件保留默认 256 条，落后的监听器收到 `RecvError::Lagged`。完整 JSON-RPC 报文默认上限 1 MiB，包含 Base64 膨胀。

事件队列不是持久收件箱。监听器缺失或落后时仍会自动确认网络接收，应用需自行实现可靠持久化和补偿同步。完整参数、范围、兼容边界见 [英文 README](README.md#bounds-and-protocol)。

## 验证

```bash
cargo fmt --all -- --check
cargo test --locked
cargo clippy --locked --all-targets -- -D warnings
cargo build --locked --examples
cargo doc --locked --no-deps
cargo package --locked
```

[examples/roundtrip.rs](examples/roundtrip.rs) 可验证真实服务端双向 Unicode 消息、心跳、重连与清理；提供 `WK_PEER_TOKEN` 时创建第二个 Rust 客户端，省略时使用 [JS 对端脚本](tests/interop.mjs)。需要事先由业务后端分别注册身份。精确验证记录和未覆盖范围见 [docs/VALIDATION.md](docs/VALIDATION.md)。


## WSS 私有证书与持续验收

私有 CA 可通过 `Options.additional_root_certificates` 添加 DER 根证书；最多
16 张，每张不超过 64 KiB，不接收私钥。公共 WebPKI 根仍保留，域名与有效期校验始终启用。
五项 TLS 测试覆盖可信 CA、不可信 CA、错误域名、过期证书和无效配置。

CI 自动构建固定版本的 WuKongIM 单节点集群（256 Hash Slot、开启 Token 校验），
先验证 Rust/Rust 收发和错误 Token 拒绝，再与真实 npm `easyjssdk@2.0.4` 进行
120 秒 WSS Unicode 收发。期间中断 Rust 连接三次，每次都要求自动重连后恢复收发。
手动运行 `Real server acceptance` 工作流可选择 600 秒。

```bash
git clone https://github.com/WuKongIM/WuKongIM.git test-server
git -C test-server checkout 27a39f15bf163b433f417b78ab6bfc6e589585e5
python3 tests/acceptance/run.py --server-source test-server --seconds 120
```

需要 Rust 1.86+、Go 1.25.11、Node 22.12+ 与 npm、Python 3.11+、OpenSSL。
测试使用临时 CA、合成身份、回环监听，并清理自己启动的进程。
成功回执 `.acceptance/receipt.json` 记录精确源码、消息数量、断网恢复与清理；
该有限时长验收不代表容量或多日稳定性结论。


## 正式发布包验收

真实服务端验收工具可以作为 `wukong-easy-sdk = "=0.1.0"` 的独立消费者构建探针。
Registry 模式从空 Cargo 缓存开始，核对下载包的 SHA-256 和源码身份，不使用本地
路径或 Git SDK 依赖。CI 分别保存源码模式与正式包模式的回执。

```sh
python3 tests/acceptance/run.py --server-source test-server --distribution registry --seconds 120 --output .acceptance/registry.json
```

服务端必须检出文档指定的精确 revision。正式包回执分别记录发布包源码与验收工具源码，
避免把新工具的 commit 当成已发布包的版本。
