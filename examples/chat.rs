//! Run two terminals with distinct backend-issued identities and reciprocal WK_PEER_UID.
use serde_json::json;
use tokio::io::{AsyncBufReadExt, BufReader};
use wukong_easy_sdk::{Auth, ChannelType, Client, Event, Options};

#[tokio::main]
async fn main() {
    if run().await.is_err() {
        eprintln!("Chat operation failed; check configuration and connection state.");
        std::process::exit(1);
    }
}
async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::new(
        std::env::var("WK_WS_URL")?,
        Auth::new(std::env::var("WK_UID")?, std::env::var("WK_TOKEN")?),
        Options::default(),
    )?;
    let peer = std::env::var("WK_PEER_UID")?;
    let mut events = client.subscribe();
    let result = async {
        client.connect().await?;
        println!("Connected. Type a message; /quit exits. Message bodies are not logged.");
        let mut input = BufReader::new(tokio::io::stdin()).lines();
        loop {
            tokio::select! {
                line = input.next_line() => {
                    let Some(line) = line? else { break; };
                    if line == "/quit" { break; }
                    match client.send(&peer, ChannelType::Person, json!({"type":1,"content":line})).await {
                        Ok(_) => println!("SEND accepted by server"),
                        Err(_) => eprintln!("SEND failed; acceptance may be unknown"),
                    }
                }
                event = events.recv() => match event {
                    Ok(Event::Message(message)) => {
                        // Render message.payload in your application's UI; avoid logging its body.
                        let _ = &message.payload;
                        println!("Message received");
                    }
                    Ok(Event::Reconnecting{..}) => println!("Reconnecting"),
                    Ok(Event::Connect(_)) => println!("Authenticated"),
                    Ok(Event::Disconnect(_)) => println!("Disconnected"),
                    Ok(Event::Error(_)) => eprintln!("Connection operation failed"),
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                        eprintln!("Events lost; reconcile application state"); break;
                    }
                    Err(_) => break,
                    _ => {},
                },
                _ = tokio::signal::ctrl_c() => break,
            }
        }
        Ok::<_, Box<dyn std::error::Error>>(())
    }.await;
    client.destroy().await;
    result
}
