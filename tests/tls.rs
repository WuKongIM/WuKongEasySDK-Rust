use futures_util::{SinkExt, StreamExt};
use rcgen::{BasicConstraints, CertificateParams, IsCa, KeyPair};
use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer};
use serde_json::{json, Value};
use std::{sync::Arc, time::Duration};
use tokio::{net::TcpListener, time::timeout};
use tokio_rustls::TlsAcceptor;
use tokio_tungstenite::{accept_async, tungstenite::Message};
use wukong_easy_sdk::{Auth, Client, Error, Options};

fn certificates(
    host: &str,
    expired: bool,
) -> (
    Vec<u8>,
    CertificateDer<'static>,
    PrivatePkcs8KeyDer<'static>,
) {
    let key = KeyPair::generate().unwrap();
    let mut params = CertificateParams::new(vec![]).unwrap();
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    let ca = params.self_signed(&key).unwrap();
    let leaf_key = KeyPair::generate().unwrap();
    let mut leaf_params = CertificateParams::new(vec![host.to_owned()]).unwrap();
    if expired {
        leaf_params.not_before = rcgen::date_time_ymd(2000, 1, 1);
        leaf_params.not_after = rcgen::date_time_ymd(2001, 1, 1);
    }
    let leaf = leaf_params.signed_by(&leaf_key, &ca, &key).unwrap();
    (
        ca.der().to_vec(),
        leaf.der().clone(),
        leaf_key.serialize_der().into(),
    )
}

async fn check_tls(host: &str, expired: bool, trusted: bool, accepted: bool) {
    let (ca, leaf, key) = certificates(host, expired);
    let config = rustls::ServerConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_no_client_auth()
    .with_single_cert(vec![leaf], key.into())
    .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("wss://127.0.0.1:{}", listener.local_addr().unwrap().port());
    let server = tokio::spawn(async move {
        let (tcp, _) = listener.accept().await.unwrap();
        let tls = TlsAcceptor::from(Arc::new(config)).accept(tcp).await;
        if !accepted {
            assert!(tls.is_err());
            return;
        }
        let mut ws = accept_async(tls.unwrap()).await.unwrap();
        let Message::Text(text) = ws.next().await.unwrap().unwrap() else {
            panic!("expected CONNECT")
        };
        let request: Value = serde_json::from_str(&text).unwrap();
        assert_eq!(request["method"], "connect");
        ws.send(Message::Text(
            json!({"id":request["id"],"result":{"reasonCode":1}})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
        let _ = ws.next().await;
    });
    let client = Client::new(
        url,
        Auth::new("tls-user", "synthetic-token"),
        Options {
            connect_timeout: Duration::from_secs(2),
            additional_root_certificates: if trusted { vec![ca] } else { vec![] },
            ..Options::default()
        },
    )
    .unwrap();
    let result = timeout(Duration::from_secs(3), client.connect())
        .await
        .unwrap();
    if accepted {
        assert_eq!(result.unwrap().reason_code, 1);
    } else {
        assert_eq!(result.unwrap_err(), Error::Transport);
    }
    client.destroy().await;
    timeout(Duration::from_secs(3), server)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn trusted_wss_validates_and_authenticates() {
    check_tls("127.0.0.1", false, true, true).await;
}
#[tokio::test]
async fn untrusted_ca_is_rejected() {
    check_tls("127.0.0.1", false, false, false).await;
}
#[tokio::test]
async fn trusted_ca_does_not_bypass_hostname_validation() {
    check_tls("wrong.example", false, true, false).await;
}
#[tokio::test]
async fn trusted_ca_does_not_bypass_expiry_validation() {
    check_tls("127.0.0.1", true, true, false).await;
}
#[test]
fn invalid_or_excessive_root_input_fails_before_io() {
    for roots in [vec![vec![1, 2, 3]], vec![vec![0; 65537]], vec![vec![1]; 17]] {
        assert!(matches!(
            Client::new(
                "wss://localhost",
                Auth::new("a", "t"),
                Options {
                    additional_root_certificates: roots,
                    ..Options::default()
                }
            ),
            Err(Error::InvalidInput(_))
        ));
    }
}
