use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{fmt, sync::Arc, time::Duration};

/// Device category. Token registration must use this same wire value.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[repr(u8)]
pub enum DeviceFlag {
    App = 0,
    Web = 1,
    #[default]
    Desktop = 2,
}

/// Known Channel types; accepting `u8` also preserves future server extensions.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum ChannelType {
    Person = 1,
    Group = 2,
    CustomerService = 3,
    Community = 4,
    CommunityTopic = 5,
    Info = 6,
    Data = 7,
    Temp = 8,
    Live = 9,
    Visitors = 10,
}
impl From<ChannelType> for u8 {
    fn from(value: ChannelType) -> Self {
        value as u8
    }
}

/// Known numeric reasons shared with EasyJSSDK 2.0.4. Results and errors retain
/// raw i64 codes so future and business-defined server values are not discarded.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(i64)]
pub enum ReasonCode {
    Unknown = 0,
    Success = 1,
    AuthFail = 2,
    SubscriberNotExist = 3,
    InBlacklist = 4,
    ChannelNotExist = 5,
    UserNotOnNode = 6,
    SenderOffline = 7,
    MsgKeyError = 8,
    PayloadDecodeError = 9,
    ForwardSendPacketError = 10,
    NotAllowSend = 11,
    ConnectKick = 12,
    NotInWhitelist = 13,
    QueryTokenError = 14,
    SystemError = 15,
    ChannelIdError = 16,
    NodeMatchError = 17,
    NodeNotMatch = 18,
    Ban = 19,
    NotSupportHeader = 20,
    ClientKeyIsEmpty = 21,
    RateLimit = 22,
    NotSupportChannelType = 23,
    Disband = 24,
    SendBan = 25,
}

/// Credentials supplied by a trusted application backend. Debug redacts all fields.
#[derive(Clone)]
pub struct Auth {
    pub uid: String,
    pub token: String,
    /// Stable across reconnects; generated once by `Auth::new`.
    pub device_id: String,
    /// Defaults to PC/Desktop (`2`), unlike the JavaScript SDK's WEB (`1`).
    pub device_flag: DeviceFlag,
}
impl Auth {
    pub fn new(uid: impl Into<String>, token: impl Into<String>) -> Self {
        Self {
            uid: uid.into(),
            token: token.into(),
            device_id: uuid::Uuid::new_v4().to_string(),
            device_flag: DeviceFlag::Desktop,
        }
    }
}
impl fmt::Debug for Auth {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Auth([REDACTED])")
    }
}

/// Resource and deadline limits for one identity's connection.
#[derive(Clone, Debug)]
pub struct Options {
    /// Total TCP/TLS/WebSocket and CONNECT authentication deadline.
    pub connect_timeout: Duration,
    /// Total SEND deadline, including its bounded local queue and server response.
    pub request_timeout: Duration,
    /// Maximum time a transport write may block the reader.
    pub write_timeout: Duration,
    /// Interval between JSON-RPC heartbeat requests after authentication.
    pub ping_interval: Duration,
    /// Deadline for the matching ping result (including `result: null`).
    pub pong_timeout: Duration,
    /// Automatic retries after an established connection fails; zero disables them.
    pub max_reconnect_attempts: u32,
    /// Exponential backoff starts here, with up to 25% jitter.
    pub reconnect_delay: Duration,
    /// Hard upper bound including jitter.
    pub max_reconnect_delay: Duration,
    /// Bounds queued and outstanding sends across all cloned client handles.
    pub max_in_flight: usize,
    /// Retained event count, rounded up to a power of two by Tokio broadcast.
    /// Slow receivers get `RecvError::Lagged`; there is no durable replay.
    pub event_capacity: usize,
    /// Maximum complete incoming or outgoing JSON-RPC message size in bytes.
    pub max_message_size: usize,
}
impl Default for Options {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(15),
            write_timeout: Duration::from_secs(5),
            ping_interval: Duration::from_secs(25),
            pong_timeout: Duration::from_secs(10),
            max_reconnect_attempts: 5,
            reconnect_delay: Duration::from_secs(1),
            max_reconnect_delay: Duration::from_secs(30),
            max_in_flight: 256,
            event_capacity: 256,
            max_message_size: 1024 * 1024,
        }
    }
}
impl Options {
    pub(crate) fn validate(&self) -> Result<(), Error> {
        let durations = [
            self.connect_timeout,
            self.request_timeout,
            self.write_timeout,
            self.ping_interval,
            self.pong_timeout,
            self.reconnect_delay,
            self.max_reconnect_delay,
        ];
        if durations
            .iter()
            .any(|d| d.is_zero() || *d > Duration::from_secs(86400))
            || self.max_reconnect_delay < self.reconnect_delay
            || self.max_reconnect_attempts > 100
            || !(1..=65536).contains(&self.max_in_flight)
            || !(1..=65536).contains(&self.event_capacity)
            || !(256..=64 * 1024 * 1024).contains(&self.max_message_size)
        {
            return Err(Error::InvalidInput("invalid resource or duration limit"));
        }
        Ok(())
    }
}

/// Wire flags. SEND defaults `red_dot` to true; explicit false is respected.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Header {
    #[serde(default)]
    pub no_persist: bool,
    #[serde(default)]
    pub red_dot: bool,
    #[serde(default)]
    pub sync_once: bool,
    #[serde(default)]
    pub dup: bool,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct MessageSetting {
    #[serde(default)]
    pub receipt: bool,
    #[serde(default)]
    pub signal: bool,
    #[serde(default)]
    pub stream: bool,
    #[serde(default)]
    pub topic: bool,
}

/// SEND extensions; the backend must configure group membership separately.
#[derive(Clone, Debug)]
pub struct SendOptions {
    /// Application retry identity. Generated per send when omitted; never auto-replayed.
    pub client_msg_no: Option<String>,
    pub header: Header,
    pub setting: Option<MessageSetting>,
    pub topic: Option<String>,
}
impl Default for SendOptions {
    fn default() -> Self {
        Self {
            client_msg_no: None,
            header: Header {
                red_dot: true,
                ..Header::default()
            },
            setting: None,
            topic: None,
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ConnectResult {
    pub reason_code: i64,
    #[serde(default)]
    pub server_key: String,
    #[serde(default)]
    pub salt: String,
    #[serde(default)]
    pub time_diff: i64,
    #[serde(default)]
    pub server_version: u32,
    #[serde(default)]
    pub node_id: u64,
}

/// Server acceptance only; does not imply recipient delivery or business success.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct SendResult {
    pub message_id: String,
    pub message_seq: u64,
    pub reason_code: i64,
}

/// Received content, including application-controlled payload. Do not log whole events.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RecvMessage {
    #[serde(default)]
    pub header: Header,
    pub message_id: String,
    pub message_seq: u64,
    pub timestamp: i64,
    pub channel_id: String,
    pub channel_type: u8,
    pub from_uid: String,
    pub payload: Value,
    #[serde(default)]
    pub client_msg_no: Option<String>,
    #[serde(default)]
    pub setting: Option<MessageSetting>,
    #[serde(default)]
    pub topic: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct CustomEvent {
    #[serde(default)]
    pub header: Option<Header>,
    pub id: String,
    #[serde(rename = "type")]
    pub event_type: String,
    pub timestamp: i64,
    pub data: Value,
}

/// One terminal transition per session/run. Server reasons are exposed as numeric codes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DisconnectReason {
    Manual,
    Transport,
    HeartbeatTimeout,
    Server { code: i64 },
}

/// Bounded broadcast events; message/event bodies are shared across subscribers.
#[derive(Clone, Debug)]
pub enum Event {
    Connect(ConnectResult),
    Disconnect(DisconnectReason),
    Message(Arc<RecvMessage>),
    SendAck(SendResult),
    Reconnecting { attempt: u32, delay: Duration },
    CustomEvent(Arc<CustomEvent>),
    Error(Error),
}

/// Safe to format: errors never retain tokens, URLs, payloads or raw server text.
#[derive(Clone, Debug, thiserror::Error, PartialEq, Eq)]
pub enum Error {
    #[error("invalid input: {0}")]
    InvalidInput(&'static str),
    #[error("client is not connected")]
    NotConnected,
    #[error("client was disconnected")]
    Disconnected,
    #[error("client was destroyed")]
    Destroyed,
    #[error("too many outstanding sends")]
    Backpressure,
    #[error("operation timed out")]
    Timeout,
    #[error("WebSocket transport failed")]
    Transport,
    #[error("invalid protocol message: {0}")]
    Protocol(&'static str),
    #[error("server rejected operation (code {code})")]
    Server { code: i64 },
    #[error("maximum reconnect attempts reached")]
    ReconnectExhausted,
    #[error("message exceeds configured size limit")]
    MessageTooLarge,
    #[error("connect requires a Tokio runtime")]
    RuntimeUnavailable,
}
