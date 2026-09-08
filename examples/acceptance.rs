//! Sustained WSS interoperability probe for tests/acceptance/run.py.
//! Credentials come from the harness; this client never calls Product HTTP.
use serde_json::{json, Value};
use std::{
    collections::HashSet,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{
    sync::mpsc,
    time::{sleep, timeout, Instant},
};
use wukong_easy_sdk::{Auth, ChannelType, Client, Error, Event, Options};

#[derive(Default)]
struct Observed {
    connects: AtomicU64,
    disconnects: AtomicU64,
    failures: AtomicU64,
    lagged: AtomicBool,
}

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("Acceptance probe failed: {error}");
        std::process::exit(1);
    }
}
async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let seconds: u64 = std::env::var("WK_TEST_SECONDS")?.parse()?;
    if !(10..=900).contains(&seconds) {
        return Err("invalid duration".into());
    }
    let peer_uid = std::env::var("WK_PEER_UID")?;
    let mut options = Options {
        ping_interval: Duration::from_millis(250),
        pong_timeout: Duration::from_secs(1),
        connect_timeout: Duration::from_secs(3),
        request_timeout: Duration::from_secs(3),
        max_reconnect_attempts: 20,
        reconnect_delay: Duration::from_millis(100),
        max_reconnect_delay: Duration::from_millis(500),
        ..Options::default()
    };
    if let Ok(path) = std::env::var("WK_TEST_CA_DER") {
        options
            .additional_root_certificates
            .push(std::fs::read(path)?);
    }
    let client = Client::new(
        std::env::var("WK_WS_URL")?,
        Auth::new(std::env::var("WK_UID")?, std::env::var("WK_TOKEN")?),
        options,
    )?;
    let mut events = client.subscribe();
    let observed = Arc::new(Observed::default());
    let stats = observed.clone();
    let (messages, mut incoming) = mpsc::channel(256);
    let observer = tokio::spawn(async move {
        loop {
            match events.recv().await {
                Ok(Event::Connect(_)) => {
                    stats.connects.fetch_add(1, Ordering::Relaxed);
                }
                Ok(Event::Disconnect(_)) => {
                    stats.disconnects.fetch_add(1, Ordering::Relaxed);
                }
                Ok(Event::Message(message)) => {
                    if messages.send(message).await.is_err() {
                        break;
                    }
                }
                Ok(Event::Error(_)) => {
                    stats.failures.fetch_add(1, Ordering::Relaxed);
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                    stats.lagged.store(true, Ordering::Relaxed);
                    break;
                }
                Err(_) => break,
                _ => {}
            }
        }
    });
    let result = async {
        client.connect().await?;
        let started=Instant::now();
        let mut sent=0_u64;
        let mut completed=0_u64;
        let mut interrupted=0_u64;
        let mut last_recovered=0;
        let mut received=HashSet::new();
        let mut issued=HashSet::new();
        while started.elapsed() < Duration::from_secs(seconds) {
            if !client.is_connected() { sleep(Duration::from_millis(20)).await;continue; }
            let before=observed.disconnects.load(Ordering::Relaxed);
            let nonce=uuid::Uuid::new_v4().to_string();
            issued.insert(nonce.clone());
            let payload=json!({"type":1,"content":"WSS 你好 🦀","nonce":nonce});
            let attempt=async {
                let ack=client.send(&peer_uid,ChannelType::Person,payload.clone()).await?;
                if ack.reason_code!=1 {return Err(Error::Protocol("non-success ACK"));}
                loop {
                    let message=incoming.recv().await.ok_or(Error::Disconnected)?;
                    let candidate=message.payload.get("nonce").and_then(Value::as_str).ok_or(Error::Protocol("missing nonce"))?;
                    if message.from_uid!=peer_uid || !issued.contains(candidate) || !received.insert(candidate.to_owned())
                        || message.payload!=json!({"type":1,"content":"WSS 你好 🦀","nonce":candidate}) {
                        return Err(Error::Protocol("unexpected, duplicate or corrupt echo"));
                    }
                    if message.payload==payload {return Ok(());}
                }
            };
            sent+=1;
            match timeout(Duration::from_secs(4),attempt).await {
                Ok(Ok(()))=>{completed+=1;last_recovered=observed.connects.load(Ordering::Relaxed);},
                Ok(Err(Error::Protocol(reason)))=>return Err(Error::Protocol(reason).into()),
                failure=>{
                    // Unknown SEND outcomes are permitted only across an observed interruption.
                    tokio::task::yield_now().await;
                    if client.is_connected() && observed.disconnects.load(Ordering::Relaxed)==before {return Err(format!("unexplained online send/echo failure: {failure:?}; sent={sent}, completed={completed}, errors={}", observed.failures.load(Ordering::Relaxed)).into());}
                    interrupted+=1;
                }
            }
            sleep(Duration::from_millis(25)).await;
        }
        let connects=observed.connects.load(Ordering::Relaxed);
        let disconnects=observed.disconnects.load(Ordering::Relaxed);
        if completed < seconds || connects<4 || disconnects<3 || last_recovered!=connects || observed.lagged.load(Ordering::Relaxed) {
            return Err("insufficient sustained recovery evidence".into());
        }
        println!("{}",json!({"schema":"wukong-easy-sdk.acceptance/v1","seconds":seconds,"attempted":sent,"completed":completed,
            "interrupted":interrupted,"connects":connects,"disconnects":disconnects,"duplicates":0,"lagged":false,"status":"pass"}));
        Ok::<_,Box<dyn std::error::Error>>(())
    }.await;
    client.destroy().await;
    observer.abort();
    let _ = observer.await;
    result
}
