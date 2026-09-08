//! Repeatable live online smoke; backend must register both users first (device flag 2).
//! Omit WK_PEER_TOKEN when a JavaScript peer runs tests/interop.mjs instead.
use serde_json::json;
use std::time::Duration;
use tokio::{sync::broadcast, time::timeout};
use wukong_easy_sdk::{Auth, ChannelType, Client, Event, Options};

async fn receive(
    events: &mut broadcast::Receiver<Event>,
    sender: &str,
    payload: &serde_json::Value,
) -> Result<(), Box<dyn std::error::Error>> {
    timeout(Duration::from_secs(10), async {
        loop {
            if let Event::Message(message) = events.recv().await? {
                if message.from_uid == sender && &message.payload == payload {
                    return Ok(());
                }
            }
        }
    })
    .await?
}
#[tokio::main]
async fn main() {
    if run().await.is_err() {
        eprintln!("Online roundtrip failed");
        std::process::exit(1);
    }
}
async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let url = std::env::var("WK_WS_URL")?;
    let uid = std::env::var("WK_UID")?;
    let peer_uid = std::env::var("WK_PEER_UID")?;
    let options = Options {
        ping_interval: Duration::from_millis(50),
        pong_timeout: Duration::from_secs(2),
        ..Options::default()
    };
    let client = Client::new(
        &url,
        Auth::new(&uid, std::env::var("WK_TOKEN")?),
        options.clone(),
    )?;
    let peer = std::env::var("WK_PEER_TOKEN")
        .ok()
        .map(|token| Client::new(&url, Auth::new(&peer_uid, token), options))
        .transpose()?;
    let result = async {
        let mut incoming = client.subscribe();
        let mut peer_incoming = peer.as_ref().map(Client::subscribe);
        if let Some(peer) = &peer {
            peer.connect().await?;
        }
        client.connect().await?;
        for round in 0..2 {
            let payload =
                json!({"type":1,"content":"你好 Rust 🦀","nonce":uuid::Uuid::new_v4().to_string()});
            let ack = client
                .send(&peer_uid, ChannelType::Person, payload.clone())
                .await?;
            assert_eq!(ack.reason_code, 1);
            if let (Some(peer), Some(events)) = (&peer, &mut peer_incoming) {
                receive(events, &uid, &payload).await?;
                peer.send(&uid, ChannelType::Person, payload.clone())
                    .await?;
            }
            receive(&mut incoming, &peer_uid, &payload).await?;
            // Exercise multiple heartbeat periods against the real server.
            tokio::time::sleep(Duration::from_millis(180)).await;
            assert!(client.is_connected());
            if round == 0 {
                client.disconnect().await;
                assert!(!client.is_connected());
                client.connect().await?;
            }
        }
        Ok::<_, Box<dyn std::error::Error>>(())
    }
    .await;
    client.destroy().await;
    if let Some(peer) = peer {
        peer.destroy().await;
    }
    result?;
    println!("PASS: bidirectional Unicode messaging, SENDACK, heartbeat, reconnect and cleanup");
    Ok(())
}
