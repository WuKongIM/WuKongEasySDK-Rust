//! Bounded JSON-lines probe controlled by tests/acceptance/group.py.
//! Synthetic identities only; membership mutations belong to the trusted harness.
use serde_json::{json, Value};
use std::{collections::HashMap, error::Error as StdError, io::Write, time::Duration};
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    sync::broadcast,
    time::{sleep, timeout, Instant},
};
use wukong_easy_sdk::{Auth, ChannelType, Client, Error, Event, Options, SendResult};

type Result<T> = std::result::Result<T, Box<dyn StdError>>;
const USERS: [&str; 4] = ["group-alice", "group-bob", "group-carol", "group-dave"];

fn emit(value: Value) -> Result<()> {
    println!("{value}");
    std::io::stdout().flush()?;
    Ok(())
}

// Every message must belong to the current phase, match its SENDACK and reach
// exactly the expected clients. The exclusion window is bounded, not a proof
// that an unauthorized message can never arrive in an arbitrary deployment.
async fn observe(
    inboxes: &mut [broadcast::Receiver<Event>],
    expected: &[usize],
    sender: usize,
    channel: &str,
    payload: &Value,
    ack: Option<&SendResult>,
) -> Result<usize> {
    let started = Instant::now();
    let deadline = started + Duration::from_secs(8);
    let mut seen = [false; 4];
    loop {
        for (index, inbox) in inboxes.iter_mut().enumerate() {
            loop {
                match inbox.try_recv() {
                    Ok(Event::Message(message)) => {
                        if !expected.contains(&index) || seen[index] {
                            return Err(format!("unexpected or duplicate recipient {index}").into());
                        }
                        let ack = ack.ok_or("rejected send was delivered")?;
                        if message.channel_type != u8::from(ChannelType::Group)
                            || message.channel_id != channel
                            || message.from_uid != USERS[sender]
                            || message.payload != *payload
                            || message.message_id != ack.message_id
                            || message.message_seq != ack.message_seq
                        {
                            return Err(format!(
                                "message metadata/payload mismatch at recipient {index}"
                            )
                            .into());
                        }
                        seen[index] = true;
                    }
                    Ok(Event::Error(error)) => return Err(error.into()),
                    Ok(Event::Disconnect(_)) => {
                        return Err("unexpected disconnect during group delivery".into())
                    }
                    Ok(_) => {}
                    Err(broadcast::error::TryRecvError::Empty) => break,
                    Err(error) => return Err(error.into()),
                }
            }
        }
        let complete = expected.iter().all(|index| seen[*index]);
        if complete && started.elapsed() >= Duration::from_millis(500) {
            return Ok(seen.into_iter().filter(|item| *item).count());
        }
        if Instant::now() >= deadline {
            return Err("group recipients did not complete before deadline".into());
        }
        sleep(Duration::from_millis(5)).await;
    }
}

async fn control(clients: &[Client], inboxes: &mut [broadcast::Receiver<Event>]) -> Result<()> {
    let mut lines = BufReader::new(tokio::io::stdin()).lines();
    let mut sequences = HashMap::<String, u64>::new();
    emit(json!({"status":"ready","clients":clients.len()}))?;
    while let Some(line) = lines.next_line().await? {
        if line.len() > 4096 {
            return Err("control command too large".into());
        }
        let command: Value = serde_json::from_str(&line)?;
        match command["op"].as_str().ok_or("missing operation")? {
            "send" => {
                let sender = command["sender"].as_u64().ok_or("missing sender")? as usize;
                let client = clients.get(sender).ok_or("invalid sender")?;
                let channel = command["channel"].as_str().ok_or("missing channel")?;
                let phase = command["phase"].as_str().ok_or("missing phase")?;
                let reason = command["reason"].as_i64().ok_or("missing reason")?;
                let expected: Vec<usize> = command["recipients"]
                    .as_array()
                    .ok_or("missing recipients")?
                    .iter()
                    .map(|v| {
                        v.as_u64()
                            .map(|i| i as usize)
                            .filter(|i| *i < 4)
                            .ok_or("invalid recipient")
                    })
                    .collect::<std::result::Result<_, _>>()?;
                let payload = json!({"type":1,"content":"群消息 Rust 🦀","phase":phase,"nonce":uuid::Uuid::new_v4().to_string()});
                let ack = match client
                    .send(channel, ChannelType::Group, payload.clone())
                    .await
                {
                    Ok(ack) if reason == 1 && ack.reason_code == 1 => {
                        if ack.message_id.is_empty()
                            || ack.message_id == "0"
                            || ack.message_seq == 0
                            || sequences
                                .get(channel)
                                .is_some_and(|last| ack.message_seq <= *last)
                        {
                            return Err("invalid or non-increasing SENDACK identity".into());
                        }
                        sequences.insert(channel.to_owned(), ack.message_seq);
                        Some(ack)
                    }
                    Err(Error::Server { code })
                        if reason != 1 && code == reason && expected.is_empty() =>
                    {
                        None
                    }
                    other => {
                        return Err(format!("unexpected send outcome for {phase}: {other:?}").into())
                    }
                };
                let received =
                    observe(inboxes, &expected, sender, channel, &payload, ack.as_ref()).await?;
                emit(json!({"status":"pass","phase":phase,"reason":reason,"deliveries":received}))?;
            }
            "arm_reconnect" => {
                for inbox in inboxes.iter_mut() {
                    loop {
                        match inbox.try_recv() {
                            Ok(Event::Message(_)) => {
                                return Err("late group message before interruption".into())
                            }
                            Ok(_) => {}
                            Err(broadcast::error::TryRecvError::Empty) => break,
                            Err(error) => return Err(error.into()),
                        }
                    }
                }
                emit(json!({"status":"armed"}))?;
            }
            "wait_reconnect" => {
                for inbox in inboxes.iter_mut() {
                    timeout(Duration::from_secs(12), async {
                        let mut disconnected = false;
                        loop {
                            match inbox.recv().await? {
                                Event::Disconnect(_) => disconnected = true,
                                Event::Connect(_) if disconnected => {
                                    return Ok::<_, Box<dyn StdError>>(())
                                }
                                Event::Message(_) => {
                                    return Err("unexpected message during interruption".into())
                                }
                                _ => {}
                            }
                        }
                    })
                    .await??;
                }
                if !clients.iter().all(Client::is_connected) {
                    return Err("client failed to reconnect".into());
                }
                emit(json!({"status":"reconnected","clients":clients.len()}))?;
            }
            "stop" => return Ok(()),
            _ => return Err("unknown operation".into()),
        }
    }
    Err("control stream ended before stop".into())
}

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("Group acceptance failed: {error}");
        std::process::exit(1);
    }
}

async fn run() -> Result<()> {
    let options = Options {
        additional_root_certificates: vec![std::fs::read(std::env::var("WK_TEST_CA_DER")?)?],
        ping_interval: Duration::from_millis(250),
        pong_timeout: Duration::from_secs(2),
        reconnect_delay: Duration::from_millis(100),
        max_reconnect_delay: Duration::from_millis(500),
        max_reconnect_attempts: 20,
        ..Options::default()
    };
    let url = std::env::var("WK_WS_URL")?;
    let clients: Vec<Client> = USERS
        .iter()
        .map(|uid| {
            Client::new(
                &url,
                Auth::new(*uid, format!("{uid}-synthetic-token")),
                options.clone(),
            )
        })
        .collect::<std::result::Result<_, _>>()?;
    let mut inboxes: Vec<_> = clients.iter().map(Client::subscribe).collect();
    let result = async {
        for client in &clients {
            client.connect().await?;
        }
        control(&clients, &mut inboxes).await
    }
    .await;
    for client in &clients {
        client.destroy().await;
    }
    result?;
    emit(json!({"status":"stopped","clients_destroyed":clients.len()}))
}
