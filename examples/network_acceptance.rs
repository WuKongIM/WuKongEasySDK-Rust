//! Public-API weak-network and resource probe driven by tests/acceptance/network.py.
//! All credentials are synthetic; only the trusted harness calls Product HTTP.
use serde_json::{json, Value};
use std::{error::Error as StdError, future::Future, io::Write, time::Duration};
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    sync::broadcast,
    time::{sleep, timeout, Instant},
};
use wukong_easy_sdk::{
    Auth, ChannelType, Client, DisconnectReason, Error, Event, Options, RecvMessage,
};

type Result<T> = std::result::Result<T, Box<dyn StdError>>;
const USERS: [&str; 2] = ["network-alice", "network-bob"];

fn emit(value: Value) -> Result<()> {
    println!("{value}");
    std::io::stdout().flush()?;
    Ok(())
}

struct Pair {
    clients: [Client; 2],
    inboxes: [broadcast::Receiver<Event>; 2],
    delivered: u64,
}

impl Pair {
    fn new() -> Result<Self> {
        let options = Options {
            additional_root_certificates: vec![std::fs::read(std::env::var("WK_TEST_CA_DER")?)?],
            connect_timeout: Duration::from_secs(2),
            request_timeout: Duration::from_millis(800),
            ping_interval: Duration::from_millis(250),
            pong_timeout: Duration::from_secs(3),
            reconnect_delay: Duration::from_millis(100),
            max_reconnect_delay: Duration::from_millis(300),
            max_reconnect_attempts: 30,
            max_in_flight: 2,
            event_capacity: 16,
            ..Options::default()
        };
        let urls = [
            std::env::var("WK_WS_URL")?,
            std::env::var("WK_PEER_WS_URL")?,
        ];
        let create = |index: usize| {
            Client::new(
                &urls[index],
                Auth::new(USERS[index], format!("{}-synthetic-token", USERS[index])),
                options.clone(),
            )
        };
        let clients = [create(0)?, create(1)?];
        let inboxes = [clients[0].subscribe(), clients[1].subscribe()];
        Ok(Self {
            clients,
            inboxes,
            delivered: 0,
        })
    }

    async fn destroy(&self) {
        for client in &self.clients {
            client.destroy().await;
        }
    }

    async fn receive(
        &mut self,
        from: usize,
        payload: &Value,
    ) -> Result<std::sync::Arc<RecvMessage>> {
        let message = timeout(Duration::from_secs(3), async {
            loop {
                if let Event::Message(message) = self.inboxes[1 - from].recv().await? {
                    return Ok::<_, broadcast::error::RecvError>(message);
                }
            }
        })
        .await??;
        if message.from_uid != USERS[from]
            || message.channel_id != USERS[from]
            || message.channel_type != u8::from(ChannelType::Person)
            || message.payload != *payload
            || message.message_id.is_empty()
            || message.message_id == "0"
            || message.message_seq == 0
        {
            return Err("unexpected, duplicate or corrupt delivery".into());
        }
        self.delivered += 1;
        Ok(message)
    }

    // A finite observation window catches buffered duplicate delivery and late
    // replay. Subsequent phases also reject any payload from a previous phase.
    async fn quiet(&mut self) -> Result<()> {
        let deadline = Instant::now() + Duration::from_millis(150);
        loop {
            for inbox in &mut self.inboxes {
                loop {
                    match inbox.try_recv() {
                        Ok(Event::Message(_)) => {
                            return Err("unexpected late delivery or automatic replay".into())
                        }
                        Ok(_) => {}
                        Err(broadcast::error::TryRecvError::Empty) => break,
                        Err(error) => return Err(error.into()),
                    }
                }
            }
            if Instant::now() >= deadline {
                return Ok(());
            }
            sleep(Duration::from_millis(5)).await;
        }
    }

    async fn exchange(&mut self, count: u64) -> Result<()> {
        for index in 0..count {
            let from = (index % 2) as usize;
            let payload =
                json!({"type":1,"content":"弱网 Rust 🦀","nonce":uuid::Uuid::new_v4().to_string()});
            let ack = self.clients[from]
                .send(USERS[1 - from], ChannelType::Person, payload.clone())
                .await?;
            let message = self.receive(from, &payload).await?;
            if ack.reason_code != 1
                || ack.message_id != message.message_id
                || ack.message_seq != message.message_seq
            {
                return Err("SENDACK does not identify the delivered message".into());
            }
        }
        self.quiet().await
    }

    async fn blocked_ack(&mut self) -> Result<()> {
        // The proxy blocks only server -> Alice. Both accepted SENDs can reach
        // Bob while Alice retains two pending requests and cannot see SENDACK.
        let payloads = [
            json!({"type":1,"nonce":uuid::Uuid::new_v4().to_string()}),
            json!({"type":1,"nonce":uuid::Uuid::new_v4().to_string()}),
        ];
        let first_client = self.clients[0].clone();
        let second_client = self.clients[0].clone();
        let first = first_client.send(USERS[1], ChannelType::Person, payloads[0].clone());
        let second = second_client.send(USERS[1], ChannelType::Person, payloads[1].clone());
        tokio::pin!(first, second);
        // Poll each request into the SDK before waiting for peer delivery. This
        // avoids relying on spawned-task scheduling to occupy admission slots.
        for request in [&mut first, &mut second] {
            std::future::poll_fn(|cx| match request.as_mut().poll(cx) {
                std::task::Poll::Pending => std::task::Poll::Ready(Ok(())),
                std::task::Poll::Ready(_) => {
                    std::task::Poll::Ready(Err("blocked request completed before admission check"))
                }
            })
            .await?;
        }
        let rejected = self.clients[0]
            .send(
                USERS[1],
                ChannelType::Person,
                json!({"type":1,"must_not_deliver":true}),
            )
            .await;
        if !matches!(rejected, Err(Error::Backpressure)) {
            return Err("full admission did not return Backpressure".into());
        }
        for payload in &payloads {
            self.receive(0, payload).await?;
        }
        let (a, b) = tokio::join!(first, second);
        if !matches!(a, Err(Error::Timeout)) || !matches!(b, Err(Error::Timeout)) {
            return Err("lost SENDACK did not preserve timeout semantics".into());
        }
        Ok(())
    }

    async fn lag(&mut self) -> Result<u64> {
        let mut slow = self.clients[1].subscribe();
        for _ in 0..48 {
            let payload = json!({"type":1,"nonce":uuid::Uuid::new_v4().to_string()});
            let ack = self.clients[0]
                .send(USERS[1], ChannelType::Person, payload.clone())
                .await?;
            let message = self.receive(0, &payload).await?;
            if ack.message_id != message.message_id || ack.message_seq != message.message_seq {
                return Err("slow-observer phase metadata mismatch".into());
            }
            // Alice's normal observer must not become an unintended slow consumer.
            loop {
                match self.inboxes[0].try_recv() {
                    Ok(Event::Message(_)) => return Err("unexpected lag-phase recipient".into()),
                    Ok(_) => {}
                    Err(broadcast::error::TryRecvError::Empty) => break,
                    Err(error) => return Err(error.into()),
                }
            }
        }
        let missed = match slow.try_recv() {
            Err(broadcast::error::TryRecvError::Lagged(missed)) if missed >= 32 => missed,
            _ => return Err("slow observer did not explicitly report Lagged".into()),
        };
        self.quiet().await?;
        Ok(missed)
    }

    async fn disconnected(&mut self, heartbeat: bool) -> Result<()> {
        timeout(Duration::from_secs(8), async {
            loop {
                match self.inboxes[0].recv().await? {
                    Event::Disconnect(reason) => {
                        if heartbeat && !matches!(reason, DisconnectReason::HeartbeatTimeout) {
                            return Err("blackhole did not cause a heartbeat timeout".into());
                        }
                        return Ok::<_, Box<dyn StdError>>(());
                    }
                    Event::Message(_) => return Err("unexpected fault-phase message".into()),
                    _ => {}
                }
            }
        })
        .await?
    }

    async fn recovered(&mut self) -> Result<()> {
        timeout(Duration::from_secs(10), async {
            loop {
                match self.inboxes[0].recv().await? {
                    Event::Connect(_) if self.clients[0].is_connected() => {
                        return Ok::<_, Box<dyn StdError>>(())
                    }
                    Event::Message(_) => return Err("old SEND replayed on recovery".into()),
                    _ => {}
                }
            }
        })
        .await?
    }
}

async fn control(pair: &mut Option<Pair>) -> Result<()> {
    let mut lines = BufReader::new(tokio::io::stdin()).lines();
    emit(json!({"status":"ready"}))?;
    while let Some(line) = lines.next_line().await? {
        if line.len() > 4096 {
            return Err("control command too large".into());
        }
        let command: Value = serde_json::from_str(&line)?;
        let op = command["op"].as_str().ok_or("missing operation")?;
        let start = Instant::now();
        let mut details = json!({});
        match op {
            "new" => {
                if pair.is_some() {
                    return Err("previous clients still owned".into());
                }
                *pair = Some(Pair::new()?);
                for client in &pair.as_ref().unwrap().clients {
                    client.connect().await?;
                }
            }
            "destroy" => {
                let current = pair.take().ok_or("missing clients")?;
                current.destroy().await;
                for client in &current.clients {
                    if !matches!(client.connect().await, Err(Error::Destroyed)) {
                        return Err("destroyed client remained usable".into());
                    }
                }
                details = json!({"clients_destroyed":2,"deliveries":current.delivered});
            }
            "stop" => {
                if pair.is_some() {
                    return Err("stop before destroy".into());
                }
                emit(json!({"status":"stopped"}))?;
                return Ok(());
            }
            _ => {
                let current = pair.as_mut().ok_or("missing clients")?;
                match op {
                    "exchange" => current.exchange(2).await?,
                    "blocked_ack" => {
                        current.blocked_ack().await?;
                        details = json!({"timeouts":2,"backpressure":1,"peer_deliveries":2});
                    }
                    "lag" => {
                        details = json!({"lagged":current.lag().await?});
                    }
                    "quiet" | "arm" => current.quiet().await?,
                    "disconnected" => {
                        current
                            .disconnected(command["heartbeat"].as_bool().unwrap_or(false))
                            .await?
                    }
                    "recovered" => current.recovered().await?,
                    _ => return Err("unknown operation".into()),
                }
            }
        }
        emit(
            json!({"status":"pass","op":op,"elapsed_ms":start.elapsed().as_millis(),"details":details}),
        )?;
    }
    Err("control stream ended before stop".into())
}

#[tokio::main]
async fn main() {
    let mut pair = None;
    let result = control(&mut pair).await;
    if let Some(pair) = pair {
        pair.destroy().await;
    }
    if let Err(error) = result {
        eprintln!("Network acceptance failed: {error}");
        std::process::exit(1);
    }
}
