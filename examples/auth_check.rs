//! Acceptance helper: success means the server explicitly rejected these credentials.
use wukong_easy_sdk::{Auth, Client, Error, Options, ReasonCode};
#[tokio::main]
async fn main() {
    if run().await.is_err() {
        eprintln!("Authentication rejection not proven");
        std::process::exit(1);
    }
}
async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::new(
        std::env::var("WK_WS_URL")?,
        Auth::new(std::env::var("WK_UID")?, std::env::var("WK_TOKEN")?),
        Options::default(),
    )?;
    let result = client.connect().await;
    client.destroy().await;
    if result
        != Err(Error::Server {
            code: ReasonCode::AuthFail as i64,
        })
    {
        return Err("expected authentication failure".into());
    }
    println!("AUTH_REJECTED");
    Ok(())
}
