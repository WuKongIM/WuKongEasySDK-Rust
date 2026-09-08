//! Async, bounded WuKongIM WebSocket JSON-RPC client, modeled on EasyJSSDK 2.0.4.
//!
//! Subscribe before connecting. SENDACK and automatic RECVACK confirm transport
//! operations, not application processing. There is no offline store or replay.
//! See the repository README and `examples/chat.rs` for a complete integration.
#![forbid(unsafe_code)]

mod client;
mod protocol;
mod types;

pub use client::Client;
pub use types::*;
