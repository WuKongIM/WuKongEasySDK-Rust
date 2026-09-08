use crate::{
    protocol, Auth, ConnectResult, DisconnectReason, Error, Event, Options, SendOptions, SendResult,
};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{
    net::TcpStream,
    sync::{broadcast, mpsc, oneshot, watch, Mutex, OwnedSemaphorePermit, Semaphore},
    task::JoinHandle,
    time::{self, Instant},
};
use tokio_tungstenite::{
    connect_async_with_config,
    tungstenite::{protocol::WebSocketConfig, Message},
    MaybeTlsStream, WebSocketStream,
};
use tokio_util::sync::CancellationToken;

type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;
type Reply = oneshot::Sender<Result<SendResult, Error>>;

/// Cloneable handle for one identity. Clones share one connection and bounded queues.
/// `new` is synchronous; `connect` must be called within a Tokio runtime.
#[derive(Clone)]
pub struct Client {
    inner: Arc<Inner>,
}
struct Inner {
    url: String,
    auth: Auth,
    options: Options,
    events: broadcast::Sender<Event>,
    connected: Arc<AtomicBool>,
    generation: Arc<AtomicU64>,
    destroyed: AtomicBool,
    slots: Arc<Semaphore>,
    /// Serializes creation and complete teardown, fencing old connection generations.
    running: Mutex<Option<Running>>,
}
struct Running {
    cancel: CancellationToken,
    task: JoinHandle<()>,
    status: watch::Receiver<Status>,
    first_result: watch::Receiver<Option<Result<ConnectResult, Error>>>,
    commands: mpsc::Sender<Command>,
}
#[derive(Clone)]
enum Status {
    Connecting,
    Ready(ConnectResult),
    Failed(Error),
}
struct Command {
    generation: u64,
    id: String,
    params: Value,
    deadline: Instant,
    reply: Reply,
    /// Admission stays occupied until the wire request completes or expires.
    _permit: OwnedSemaphorePermit,
}
struct Pending {
    deadline: Instant,
    reply: Reply,
    _permit: OwnedSemaphorePermit,
}
struct Worker {
    url: String,
    auth: Auth,
    options: Options,
    events: broadcast::Sender<Event>,
    connected: Arc<AtomicBool>,
    generation: Arc<AtomicU64>,
    status: watch::Sender<Status>,
    first_result: watch::Sender<Option<Result<ConnectResult, Error>>>,
    commands: mpsc::Receiver<Command>,
}

impl Drop for Inner {
    fn drop(&mut self) {
        // Worker owns no Client/Inner, so the last handle always cancels the socket.
        if let Some(run) = self.running.get_mut().take() {
            run.cancel.cancel();
        }
    }
}

impl Client {
    pub fn new(url: impl Into<String>, auth: Auth, options: Options) -> Result<Self, Error> {
        let url = url.into();
        options.validate()?;
        // tungstenite parses the complete URI before any I/O; disallow embedded credentials.
        let uri: tokio_tungstenite::tungstenite::http::Uri = url
            .parse()
            .map_err(|_| Error::InvalidInput("invalid WebSocket URL"))?;
        if !matches!(uri.scheme_str(), Some("ws" | "wss"))
            || uri.host().is_none()
            || uri.authority().is_some_and(|a| a.as_str().contains('@'))
            || url.contains('#')
        {
            return Err(Error::InvalidInput(
                "expected ws:// or wss:// URL without userinfo or fragment",
            ));
        }
        if auth.uid.trim().is_empty() || auth.token.is_empty() || auth.device_id.trim().is_empty() {
            return Err(Error::InvalidInput("uid, token and device_id are required"));
        }
        let (events, _) = broadcast::channel(options.event_capacity);
        Ok(Self {
            inner: Arc::new(Inner {
                url,
                auth,
                slots: Arc::new(Semaphore::new(options.max_in_flight)),
                options,
                events,
                connected: Arc::new(AtomicBool::new(false)),
                generation: Arc::new(AtomicU64::new(0)),
                destroyed: AtomicBool::new(false),
                running: Mutex::new(None),
            }),
        })
    }

    /// Subscribe before connecting. Handle `RecvError::Lagged` explicitly; events are not durable.
    pub fn subscribe(&self) -> broadcast::Receiver<Event> {
        self.inner.events.subscribe()
    }
    pub fn is_connected(&self) -> bool {
        self.inner.connected.load(Ordering::Acquire)
    }

    /// Authenticate once; concurrent calls join the same attempt. During automatic
    /// reconnect this waits for that run. Initial connection errors require an explicit retry.
    /// Cancelling this future does not cancel the shared attempt; call `disconnect` to do so.
    pub async fn connect(&self) -> Result<ConnectResult, Error> {
        let mut running = self.inner.running.lock().await;
        if self.inner.destroyed.load(Ordering::Acquire) {
            return Err(Error::Destroyed);
        }
        let started = running.as_ref().is_none_or(|run| run.task.is_finished());
        if started {
            tokio::runtime::Handle::try_current().map_err(|_| Error::RuntimeUnavailable)?;
            let (status, rx) = watch::channel(Status::Connecting);
            let (first_result, first_rx) = watch::channel(None);
            let (tx, commands) = mpsc::channel(self.inner.options.max_in_flight);
            let cancel = CancellationToken::new();
            let worker = Worker {
                url: self.inner.url.clone(),
                auth: self.inner.auth.clone(),
                options: self.inner.options.clone(),
                events: self.inner.events.clone(),
                connected: self.inner.connected.clone(),
                generation: self.inner.generation.clone(),
                status,
                first_result,
                commands,
            };
            let stop = cancel.clone();
            let task = tokio::spawn(worker.run(stop));
            *running = Some(Running {
                cancel,
                task,
                status: rx,
                first_result: first_rx,
                commands: tx,
            });
        }
        let run = running.as_ref().expect("run initialized");
        let mut first = run.first_result.clone();
        let initial = started || first.borrow().is_none();
        let mut status = run.status.clone();
        drop(running);
        if initial {
            loop {
                if let Some(result) = first.borrow_and_update().clone() {
                    return result;
                }
                first.changed().await.map_err(|_| Error::Disconnected)?;
            }
        }
        loop {
            match status.borrow_and_update().clone() {
                Status::Ready(result) => return Ok(result),
                Status::Failed(error) => return Err(error),
                Status::Connecting => {}
            }
            status.changed().await.map_err(|_| Error::Disconnected)?;
        }
    }

    /// Send an object/array JSON payload. SEND is never queued while disconnected or replayed.
    /// A timeout/cancel/transport error can leave acceptance unknown; retain `client_msg_no`
    /// when your application decides to retry according to server idempotency rules.
    pub async fn send(
        &self,
        channel: &str,
        channel_type: impl Into<u8>,
        payload: Value,
    ) -> Result<SendResult, Error> {
        self.send_with_options(channel, channel_type, payload, SendOptions::default())
            .await
    }

    pub async fn send_with_options(
        &self,
        channel: &str,
        channel_type: impl Into<u8>,
        payload: Value,
        options: SendOptions,
    ) -> Result<SendResult, Error> {
        if self.inner.destroyed.load(Ordering::Acquire) {
            return Err(Error::Destroyed);
        }
        if !self.is_connected() {
            return Err(Error::NotConnected);
        }
        let generation = self.inner.generation.load(Ordering::Acquire);
        let permit = self
            .inner
            .slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::Backpressure)?;
        let deadline = Instant::now() + self.inner.options.request_timeout;
        let params = protocol::send(channel, channel_type.into(), payload, options)?;
        let id = uuid::Uuid::new_v4().to_string();
        if protocol::request("send", params.clone(), &id)
            .to_string()
            .len()
            > self.inner.options.max_message_size
        {
            return Err(Error::MessageTooLarge);
        }
        let (reply, rx) = oneshot::channel();
        let running = self.inner.running.lock().await;
        if !self.is_connected() {
            return Err(Error::NotConnected);
        }
        let run = running.as_ref().ok_or(Error::NotConnected)?;
        run.commands
            .try_send(Command {
                generation,
                id,
                params,
                deadline,
                reply,
                _permit: permit,
            })
            .map_err(|e| match e {
                mpsc::error::TrySendError::Full(_) => Error::Backpressure,
                mpsc::error::TrySendError::Closed(_) => Error::Disconnected,
            })?;
        drop(running);
        time::timeout_at(deadline, rx)
            .await
            .map_err(|_| Error::Timeout)?
            .map_err(|_| Error::Disconnected)?
    }

    /// Cancel authentication, socket I/O and reconnect sleeps, then await worker shutdown.
    /// The handle may be connected again. Cancellation of this future leaves teardown active.
    pub async fn disconnect(&self) {
        let mut running = self.inner.running.lock().await;
        if let Some(run) = running.as_mut() {
            run.cancel.cancel();
            // Keep the handle in the mutex until joined so a cancelled disconnect cannot
            // let a later connect race an old worker's final state changes.
            let _ = (&mut run.task).await;
        }
        *running = None;
    }

    /// Permanently close this client and all clones. Drop receivers to release retained events.
    pub async fn destroy(&self) {
        self.inner.destroyed.store(true, Ordering::Release);
        self.disconnect().await;
    }
}

impl Worker {
    fn emit(&self, event: Event) {
        let _ = self.events.send(event);
    }
    async fn run(mut self, cancel: CancellationToken) {
        let result = tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                self.connected.store(false, Ordering::Release);
                self.emit(Event::Disconnect(DisconnectReason::Manual));
                Err(Error::Disconnected)
            }
            result = self.supervise() => result,
        };
        self.connected.store(false, Ordering::Release);
        let error = result.err().unwrap_or(Error::Disconnected);
        if self.first_result.borrow().is_none() {
            self.first_result.send_replace(Some(Err(error.clone())));
        }
        self.status.send_replace(Status::Failed(error.clone()));
        self.commands.close();
        while let Some(command) = self.commands.recv().await {
            let _ = command.reply.send(Err(error.clone()));
        }
    }

    /// Owns every socket generation; no spawned socket callbacks can outlive this loop.
    async fn supervise(&mut self) -> Result<(), Error> {
        let mut established = false;
        let mut attempt = 0;
        loop {
            if attempt > 0 {
                let delay = reconnect_delay(&self.options, attempt);
                self.emit(Event::Reconnecting { attempt, delay });
                time::sleep(delay).await;
            }
            match self.establish().await {
                Ok((mut socket, result)) => {
                    established = true;
                    attempt = 0;
                    self.generation.fetch_add(1, Ordering::AcqRel);
                    self.connected.store(true, Ordering::Release);
                    self.status.send_replace(Status::Ready(result.clone()));
                    if self.first_result.borrow().is_none() {
                        self.first_result.send_replace(Some(Ok(result.clone())));
                    }
                    self.emit(Event::Connect(result));
                    let outcome = self.session(&mut socket).await;
                    self.connected.store(false, Ordering::Release);
                    self.status.send_replace(Status::Connecting);
                    // Fail everything still queued from this socket; never replay it.
                    while let Ok(command) = self.commands.try_recv() {
                        let _ = command.reply.send(Err(Error::Disconnected));
                    }
                    match outcome {
                        Err(Error::Server { code }) => {
                            self.emit(Event::Disconnect(DisconnectReason::Server { code }));
                            return Err(Error::Server { code });
                        }
                        Err(error) => {
                            self.emit(Event::Disconnect(if error == Error::Timeout {
                                DisconnectReason::HeartbeatTimeout
                            } else {
                                DisconnectReason::Transport
                            }));
                            self.emit(Event::Error(error));
                        }
                        Ok(()) => unreachable!("session ends with a reason"),
                    }
                }
                Err(error) => {
                    self.emit(Event::Error(error.clone()));
                    // Authentication/protocol rejection is terminal, including during reconnect.
                    if !established || matches!(error, Error::Server { .. } | Error::Protocol(_)) {
                        return Err(error);
                    }
                }
            }
            if attempt >= self.options.max_reconnect_attempts {
                self.emit(Event::Error(Error::ReconnectExhausted));
                return Err(Error::ReconnectExhausted);
            }
            attempt += 1;
        }
    }

    async fn establish(&self) -> Result<(Socket, ConnectResult), Error> {
        time::timeout(self.options.connect_timeout, async {
            let config = WebSocketConfig::default()
                .max_message_size(Some(self.options.max_message_size))
                .max_frame_size(Some(self.options.max_message_size));
            let (mut socket, _) = connect_async_with_config(&self.url, Some(config), true)
                .await
                .map_err(|_| Error::Transport)?;
            let id = uuid::Uuid::new_v4().to_string();
            self.write(&mut socket, protocol::connect(&self.auth, &id))
                .await?;
            loop {
                match socket.next().await {
                    Some(Ok(Message::Text(text))) => {
                        let value = protocol::parse(&text)?;
                        if value.get("id").and_then(Value::as_str) != Some(id.as_str()) {
                            return Err(Error::Protocol("unexpected authentication response"));
                        }
                        let result: ConnectResult = protocol::decode(protocol::response(&value)?)?;
                        return Ok((socket, result));
                    }
                    Some(Ok(Message::Ping(_))) => {
                        self.flush(&mut socket).await?;
                    }
                    Some(Ok(Message::Pong(_))) => {}
                    _ => return Err(Error::Transport),
                }
            }
        })
        .await
        .map_err(|_| Error::Timeout)?
    }

    async fn write(&self, socket: &mut Socket, value: Value) -> Result<(), Error> {
        let text = value.to_string();
        if text.len() > self.options.max_message_size {
            return Err(Error::MessageTooLarge);
        }
        time::timeout(
            self.options.write_timeout,
            socket.send(Message::Text(text.into())),
        )
        .await
        .map_err(|_| Error::Transport)?
        .map_err(|_| Error::Transport)
    }
    async fn flush(&self, socket: &mut Socket) -> Result<(), Error> {
        time::timeout(self.options.write_timeout, socket.flush())
            .await
            .map_err(|_| Error::Transport)?
            .map_err(|_| Error::Transport)
    }

    /// Multiplex bounded SENDs, notifications and heartbeat deadlines on one socket.
    async fn session(&mut self, socket: &mut Socket) -> Result<(), Error> {
        let mut pending: HashMap<String, Pending> = HashMap::new();
        let mut next_ping = Instant::now() + self.options.ping_interval;
        let mut ping: Option<(String, Instant)> = None;
        loop {
            let now = Instant::now();
            pending.retain(|_, p| !p.reply.is_closed());
            let expired: Vec<_> = pending
                .iter()
                .filter(|(_, p)| p.deadline <= now)
                .map(|(id, _)| id.clone())
                .collect();
            for id in expired {
                if let Some(p) = pending.remove(&id) {
                    let _ = p.reply.send(Err(Error::Timeout));
                }
            }
            let timer = pending
                .values()
                .map(|p| p.deadline)
                .chain(std::iter::once(ping.as_ref().map_or(next_ping, |p| p.1)))
                .min()
                .expect("heartbeat deadline");
            tokio::select! {
                _ = time::sleep_until(timer) => {
                    if let Some((_, deadline)) = &ping {
                        if *deadline <= Instant::now() { return Err(Error::Timeout); }
                    } else if next_ping <= Instant::now() {
                        let id = uuid::Uuid::new_v4().to_string();
                        let deadline = Instant::now() + self.options.pong_timeout;
                        self.write(socket, protocol::request("ping", json!({}), &id)).await?;
                        ping = Some((id, deadline));
                    }
                }
                command = self.commands.recv() => {
                    let command = command.ok_or(Error::Disconnected)?;
                    if command.reply.is_closed() { continue; }
                    if command.generation != self.generation.load(Ordering::Acquire) { let _ = command.reply.send(Err(Error::Disconnected)); continue; }
                    if command.deadline <= Instant::now() { let _ = command.reply.send(Err(Error::Timeout)); continue; }
                    let Command { id, params, deadline, reply, _permit, .. } = command;
                    let wire = protocol::request("send", params, &id);
                    pending.insert(id, Pending { deadline, reply, _permit });
                    self.write(socket, wire).await?;
                }
                message = socket.next() => {
                    match message {
                        Some(Ok(Message::Text(text))) => {
                            let value = match protocol::parse(&text) { Ok(v) => v, Err(e) => { self.emit(Event::Error(e)); continue; } };
                            if let Some(id) = value.get("id").and_then(Value::as_str) {
                                if ping.as_ref().is_some_and(|p| p.0 == id) {
                                    protocol::response(&value)?;
                                    ping = None;
                                    next_ping = Instant::now() + self.options.ping_interval;
                                } else if let Some(p) = pending.remove(id) {
                                    let result: Result<SendResult, Error> = protocol::response(&value).and_then(protocol::decode);
                                    if let Ok(ack) = &result { self.emit(Event::SendAck(ack.clone())); }
                                    let _ = p.reply.send(result);
                                }
                            } else if let Some(method) = value.get("method").and_then(Value::as_str) {
                                let params = value.get("params").cloned().unwrap_or(Value::Null);
                                match method {
                                    "recv" => match protocol::recv(params) {
                                        Ok(message) => {
                                            let ack = json!({"jsonrpc":"2.0","method":"recvack","params":{"header":message.header,"messageId":message.message_id,"messageSeq":message.message_seq}});
                                            self.emit(Event::Message(Arc::new(message)));
                                            self.write(socket, ack).await?;
                                        }
                                        Err(e) => self.emit(Event::Error(e)),
                                    },
                                    "event" => match protocol::event(params) {
                                        Ok(event) => self.emit(Event::CustomEvent(Arc::new(event))),
                                        Err(e) => self.emit(Event::Error(e)),
                                    },
                                    "disconnect" => return Err(Error::Server { code: params.get("reasonCode").or_else(|| params.get("reason_code")).and_then(Value::as_i64).unwrap_or(0) }),
                                    "pong" => { ping = None; next_ping = Instant::now() + self.options.ping_interval; }
                                    _ => {}
                                }
                            } else { self.emit(Event::Error(Error::Protocol("missing response ID or method"))); }
                        }
                        Some(Ok(Message::Ping(_))) => self.flush(socket).await?,
                        Some(Ok(Message::Pong(_))) => {},
                        Some(Ok(Message::Binary(_))) => self.emit(Event::Error(Error::Protocol("expected a text message"))),
                        _ => return Err(Error::Transport),
                    }
                }
            }
        }
    }
}

fn reconnect_delay(options: &Options, attempt: u32) -> Duration {
    let base = options
        .reconnect_delay
        .saturating_mul(2_u32.saturating_pow(attempt.saturating_sub(1)))
        .min(options.max_reconnect_delay);
    let jitter_bound = base.as_millis() / 4;
    let jitter = uuid::Uuid::new_v4().as_u128() % (jitter_bound + 1);
    base.saturating_add(Duration::from_millis(jitter as u64))
        .min(options.max_reconnect_delay)
}
