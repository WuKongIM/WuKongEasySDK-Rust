use base64::{engine::general_purpose::STANDARD, Engine};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::{future::Future, time::Duration};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{broadcast, oneshot},
    time::{sleep, timeout},
};
use tokio_tungstenite::{accept_async, tungstenite::Message, WebSocketStream};
use wukong_easy_sdk::*;

type Peer = WebSocketStream<TcpStream>;
async fn bounded<T>(future: impl Future<Output = T>) -> T {
    timeout(Duration::from_secs(4), future)
        .await
        .expect("test deadline")
}
async fn listener() -> (TcpListener, String) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}", listener.local_addr().unwrap());
    (listener, url)
}
fn options() -> Options {
    Options {
        connect_timeout: Duration::from_millis(500),
        request_timeout: Duration::from_millis(200),
        reconnect_delay: Duration::from_millis(10),
        max_reconnect_delay: Duration::from_millis(20),
        ..Options::default()
    }
}
fn client(url: String, options: Options) -> Client {
    Client::new(url, Auth::new("alice", "canary-secret"), options).unwrap()
}
async fn read(peer: &mut Peer) -> Value {
    loop {
        match bounded(peer.next()).await.unwrap().unwrap() {
            Message::Text(text) => return serde_json::from_str(&text).unwrap(),
            Message::Ping(_) => peer.flush().await.unwrap(),
            _ => panic!("expected JSON text"),
        }
    }
}
async fn write(peer: &mut Peer, value: Value) {
    peer.send(Message::Text(value.to_string().into()))
        .await
        .unwrap();
}
async fn accept(listener: &TcpListener) -> (Peer, Value) {
    let (stream, _) = bounded(listener.accept()).await.unwrap();
    let mut peer = bounded(accept_async(stream)).await.unwrap();
    let connect = read(&mut peer).await;
    assert_eq!(connect["method"], "connect");
    assert_eq!(connect["params"]["uid"], "alice");
    assert_eq!(connect["params"]["deviceFlag"], 2);
    assert!(connect["params"]["clientTimestamp"].as_u64().unwrap() > 0);
    (peer, connect)
}
async fn authenticated(listener: &TcpListener) -> Peer {
    let (mut peer, connect) = accept(listener).await;
    write(&mut peer, json!({"jsonrpc":"2.0", "id":connect["id"],"result":{"reasonCode":1,"reason_code":1,"nodeId":1,"node_id":1}})).await;
    peer
}
async fn event(
    events: &mut broadcast::Receiver<Event>,
    predicate: impl Fn(&Event) -> bool,
) -> Event {
    bounded(async {
        loop {
            let e = events.recv().await.unwrap();
            if predicate(&e) {
                return e;
            }
        }
    })
    .await
}
fn recv(payload: Value) -> Value {
    json!({"method":"recv","params":{"header":{"redDot":true},"messageId":"9007199254740993","messageSeq":99,"timestamp":1,"channelId":"alice","channelType":1,"fromUid":"bob","payload":payload}})
}

#[tokio::test]
async fn connect_send_unicode_aliases_receive_ack_event_and_cleanup() {
    let (listener, url) = listener().await;
    let server = tokio::spawn(async move {
        let mut peer = authenticated(&listener).await;
        let send = read(&mut peer).await;
        assert_eq!(send["method"], "send");
        assert_eq!(send["params"]["channelId"], "bob");
        assert_eq!(send["params"]["header"]["redDot"], false);
        assert_eq!(send["params"]["clientMsgNo"], "stable-id");
        let payload: Value = serde_json::from_slice(
            &STANDARD
                .decode(send["params"]["payload"].as_str().unwrap())
                .unwrap(),
        )
        .unwrap();
        assert_eq!(payload, json!({"content":"你好 🦀"}));
        write(&mut peer, json!({"id":send["id"],"result":{"message_id":"18446744073709551615","message_seq":u64::MAX,"reason_code":1}})).await;
        for payload in [
            json!({"content":"你好 🦀","message_id":"business"}),
            json!(STANDARD.encode(r#"{"content":"你好 🦀","message_id":"business"}"#)),
        ] {
            write(&mut peer, recv(payload)).await;
            let ack = read(&mut peer).await;
            assert_eq!(ack["method"], "recvack");
            assert!(ack.get("id").is_none());
            assert_eq!(ack["params"]["messageId"], "9007199254740993");
            assert_eq!(ack["params"]["messageSeq"], 99);
        }
        write(&mut peer, json!({"method":"event","params":{"id":"e","type":"status","timestamp":1,"data":"{\"online\":true}"}})).await;
        let _ = bounded(peer.next()).await;
    });
    let client = client(url, options());
    let mut events = client.subscribe();
    assert_eq!(client.connect().await.unwrap().node_id, 1);
    let mut opts = SendOptions {
        client_msg_no: Some("stable-id".into()),
        ..SendOptions::default()
    };
    opts.header.red_dot = false;
    let ack = client
        .send_with_options(
            "bob",
            ChannelType::Person,
            json!({"content":"你好 🦀"}),
            opts,
        )
        .await
        .unwrap();
    assert_eq!(ack.message_seq, u64::MAX);
    for _ in 0..2 {
        let Event::Message(message) = event(&mut events, |e| matches!(e, Event::Message(_))).await
        else {
            unreachable!()
        };
        assert_eq!(message.payload["content"], "你好 🦀");
        assert_eq!(message.payload["message_id"], "business");
    }
    let Event::CustomEvent(e) = event(&mut events, |e| matches!(e, Event::CustomEvent(_))).await
    else {
        unreachable!()
    };
    assert_eq!(e.data["online"], true);
    client.disconnect().await;
    assert!(!client.is_connected());
    bounded(server).await.unwrap();
}

#[tokio::test]
async fn concurrent_connect_joins_one_authentication() {
    let (listener, url) = listener().await;
    let server = tokio::spawn(async move {
        let mut peer = authenticated(&listener).await;
        assert!(timeout(Duration::from_millis(60), listener.accept())
            .await
            .is_err());
        let _ = bounded(peer.next()).await;
    });
    let client = client(url, options());
    let (a, b, c) = tokio::join!(client.connect(), client.connect(), client.connect());
    assert_eq!(a.unwrap(), b.unwrap());
    assert_eq!(c.unwrap().reason_code, 1);
    sleep(Duration::from_millis(70)).await;
    client.disconnect().await;
    bounded(server).await.unwrap();
}

#[tokio::test]
async fn auth_failure_is_terminal_and_never_logs_server_text() {
    let (listener, url) = listener().await;
    let server = tokio::spawn(async move {
        let (mut peer, c) = accept(&listener).await;
        write(
            &mut peer,
            json!({"id":c["id"],"error":{"code":2,"message":"canary-secret"}}),
        )
        .await;
        assert!(timeout(Duration::from_millis(100), listener.accept())
            .await
            .is_err());
    });
    let client = client(url, options());
    assert_eq!(
        client.connect().await.unwrap_err(),
        Error::Server { code: 2 }
    );
    assert!(!client.is_connected());
    bounded(server).await.unwrap();
    client.destroy().await;
    assert_eq!(client.connect().await.unwrap_err(), Error::Destroyed);
}

#[tokio::test]
async fn disconnect_cancels_stalled_auth_and_all_connect_waiters() {
    let (listener, url) = listener().await;
    let (tx, rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut peer, _) = accept(&listener).await;
        tx.send(()).unwrap();
        let _ = bounded(peer.next()).await;
    });
    let client = client(url, options());
    let cloned = client.clone();
    let connect = tokio::spawn(async move { cloned.connect().await });
    bounded(rx).await.unwrap();
    client.disconnect().await;
    assert_eq!(
        bounded(connect).await.unwrap().unwrap_err(),
        Error::Disconnected
    );
    bounded(server).await.unwrap();
}

#[tokio::test]
async fn handshake_and_auth_share_a_bounded_deadline() {
    let (listener, url) = listener().await;
    let server = tokio::spawn(async move {
        let (_stream, _) = bounded(listener.accept()).await.unwrap();
        sleep(Duration::from_millis(150)).await;
    });
    let client = client(
        url,
        Options {
            connect_timeout: Duration::from_millis(50),
            ..options()
        },
    );
    assert_eq!(bounded(client.connect()).await.unwrap_err(), Error::Timeout);
    bounded(server).await.unwrap();
    client.disconnect().await;
}

#[tokio::test]
async fn out_of_order_results_and_rpc_failure_correlate_by_string_id() {
    let (listener, url) = listener().await;
    let server = tokio::spawn(async move {
        let mut peer = authenticated(&listener).await;
        let a = read(&mut peer).await;
        let b = read(&mut peer).await;
        write(
            &mut peer,
            json!({"id":b["id"],"error":{"code":128,"message":"denied"}}),
        )
        .await;
        write(
            &mut peer,
            json!({"id":a["id"],"result":{"messageId":"42","messageSeq":1,"reasonCode":1}}),
        )
        .await;
        let _ = bounded(peer.next()).await;
    });
    let client = client(url, options());
    client.connect().await.unwrap();
    let (a, b) = tokio::join!(
        client.send("bob", 1u8, json!({})),
        client.send("bob", 1u8, json!({}))
    );
    assert_eq!(a.unwrap().message_id, "42");
    assert_eq!(b.unwrap_err(), Error::Server { code: 128 });
    client.disconnect().await;
    bounded(server).await.unwrap();
}

#[tokio::test]
async fn pending_sends_are_bounded_expire_and_release_admission() {
    let (listener, url) = listener().await;
    let (tx, rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let mut peer = authenticated(&listener).await;
        let _ = read(&mut peer).await;
        tx.send(()).unwrap();
        let second = read(&mut peer).await;
        write(
            &mut peer,
            json!({"id":second["id"],"result":{"messageId":"2","messageSeq":2,"reasonCode":1}}),
        )
        .await;
        let _ = bounded(peer.next()).await;
    });
    let client = client(
        url,
        Options {
            max_in_flight: 1,
            ..options()
        },
    );
    client.connect().await.unwrap();
    let cloned = client.clone();
    let first = tokio::spawn(async move { cloned.send("bob", 1u8, json!({})).await });
    bounded(rx).await.unwrap();
    assert_eq!(
        client.send("bob", 1u8, json!({})).await.unwrap_err(),
        Error::Backpressure
    );
    assert_eq!(bounded(first).await.unwrap().unwrap_err(), Error::Timeout);
    // Wait for the actor's deadline cleanup, not a server response.
    let ack = bounded(async {
        loop {
            match client.send("bob", 1u8, json!({})).await {
                Err(Error::Backpressure) => tokio::task::yield_now().await,
                result => break result.unwrap(),
            }
        }
    })
    .await;
    assert_eq!(ack.message_id, "2");
    client.disconnect().await;
    bounded(server).await.unwrap();
}

#[tokio::test]
async fn heartbeat_accepts_null_result_and_reconnects_after_missing_pong() {
    let (listener, url) = listener().await;
    let (stop_tx, stop_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let mut peer = authenticated(&listener).await;
        let ping = read(&mut peer).await;
        assert_eq!(ping["method"], "ping");
        write(&mut peer, json!({"id":ping["id"],"result":null})).await;
        let ping = read(&mut peer).await;
        assert_eq!(ping["method"], "ping");
        let mut next = authenticated(&listener).await;
        let _ = bounded(stop_rx).await;
        let _ = next.close(None).await;
    });
    let client = client(
        url,
        Options {
            ping_interval: Duration::from_millis(30),
            pong_timeout: Duration::from_millis(40),
            ..options()
        },
    );
    let mut events = client.subscribe();
    client.connect().await.unwrap();
    event(&mut events, |e| {
        matches!(e, Event::Disconnect(DisconnectReason::HeartbeatTimeout))
    })
    .await;
    event(&mut events, |e| {
        matches!(e, Event::Reconnecting { attempt: 1, .. })
    })
    .await;
    event(&mut events, |e| matches!(e, Event::Connect(_))).await;
    assert!(client.is_connected());
    client.disconnect().await;
    let _ = stop_tx.send(());
    bounded(server).await.unwrap();
}

#[tokio::test]
async fn server_disconnect_never_reconnects_and_fails_pending_send() {
    let (listener, url) = listener().await;
    let server = tokio::spawn(async move {
        let mut peer = authenticated(&listener).await;
        let _ = read(&mut peer).await;
        write(
            &mut peer,
            json!({"method":"disconnect","params":{"reasonCode":12,"reason":"secret"}}),
        )
        .await;
        assert!(timeout(Duration::from_millis(100), listener.accept())
            .await
            .is_err());
    });
    let client = client(url, options());
    let mut events = client.subscribe();
    client.connect().await.unwrap();
    assert_eq!(
        client.send("bob", 1u8, json!({})).await.unwrap_err(),
        Error::Disconnected
    );
    event(&mut events, |e| {
        matches!(e, Event::Disconnect(DisconnectReason::Server { code: 12 }))
    })
    .await;
    bounded(server).await.unwrap();
    client.disconnect().await;
}

#[tokio::test]
async fn manual_disconnect_cancels_reconnect_sleep() {
    let (listener, url) = listener().await;
    let server = tokio::spawn(async move {
        let mut peer = authenticated(&listener).await;
        peer.close(None).await.unwrap();
        assert!(timeout(Duration::from_millis(150), listener.accept())
            .await
            .is_err());
    });
    let client = client(
        url,
        Options {
            reconnect_delay: Duration::from_secs(1),
            max_reconnect_delay: Duration::from_secs(2),
            ..options()
        },
    );
    let mut events = client.subscribe();
    client.connect().await.unwrap();
    event(&mut events, |e| matches!(e, Event::Reconnecting { .. })).await;
    client.disconnect().await;
    bounded(server).await.unwrap();
    assert!(!client.is_connected());
}

#[tokio::test]
async fn dropping_last_client_closes_socket_without_a_cycle() {
    let (listener, url) = listener().await;
    let server = tokio::spawn(async move {
        let mut peer = authenticated(&listener).await;
        let _ = bounded(peer.next()).await;
    });
    let client = client(url, options());
    client.connect().await.unwrap();
    drop(client);
    bounded(server).await.unwrap();
}

#[tokio::test]
async fn reconnect_budget_is_finite_and_old_sends_are_not_replayed() {
    let (listener, url) = listener().await;
    let server = tokio::spawn(async move {
        let mut peer = authenticated(&listener).await;
        let _ = read(&mut peer).await;
        drop(peer);
        for _ in 0..2 {
            let (stream, _) = bounded(listener.accept()).await.unwrap();
            drop(stream);
        }
        assert!(timeout(Duration::from_millis(80), listener.accept())
            .await
            .is_err());
    });
    let client = client(
        url,
        Options {
            max_reconnect_attempts: 2,
            ..options()
        },
    );
    let mut events = client.subscribe();
    client.connect().await.unwrap();
    assert_eq!(
        client.send("bob", 1u8, json!({})).await.unwrap_err(),
        Error::Disconnected
    );
    event(&mut events, |e| {
        matches!(e, Event::Error(Error::ReconnectExhausted))
    })
    .await;
    bounded(server).await.unwrap();
    client.disconnect().await;
}

#[tokio::test]
async fn slow_observers_get_explicit_lag_and_receive_queue_stays_bounded() {
    let (listener, url) = listener().await;
    let (tx, rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let mut peer = authenticated(&listener).await;
        for _ in 0..10 {
            write(&mut peer, recv(json!({"content":"hello"}))).await;
            assert_eq!(read(&mut peer).await["method"], "recvack");
        }
        tx.send(()).unwrap();
        let _ = bounded(peer.next()).await;
    });
    let client = client(
        url,
        Options {
            event_capacity: 2,
            ..options()
        },
    );
    let mut events = client.subscribe();
    client.connect().await.unwrap();
    bounded(rx).await.unwrap();
    assert!(matches!(
        events.recv().await,
        Err(broadcast::error::RecvError::Lagged(_))
    ));
    client.disconnect().await;
    bounded(server).await.unwrap();
}

#[tokio::test]
async fn malformed_notifications_do_not_ack_or_block_later_valid_messages() {
    let (listener, url) = listener().await;
    let server = tokio::spawn(async move {
        let mut peer = authenticated(&listener).await;
        peer.send(Message::Text("not-json".into())).await.unwrap();
        write(&mut peer, json!({"method":"recv","params":{}})).await;
        write(&mut peer, recv(json!({}))).await;
        assert_eq!(read(&mut peer).await["method"], "recvack");
        let _ = bounded(peer.next()).await;
    });
    let client = client(url, options());
    let mut events = client.subscribe();
    client.connect().await.unwrap();
    event(&mut events, |e| {
        matches!(e, Event::Error(Error::Protocol(_)))
    })
    .await;
    event(&mut events, |e| matches!(e, Event::Message(_))).await;
    client.disconnect().await;
    bounded(server).await.unwrap();
}

#[test]
fn rejects_invalid_config_urls_and_empty_credentials() {
    for url in [
        "https://example.com",
        "ws://user:secret@example.com",
        "ws://example.com/#fragment",
        "garbage",
    ] {
        assert!(Client::new(url, Auth::new("alice", "secret"), options()).is_err());
    }
    assert!(Client::new("ws://localhost", Auth::new("", "secret"), options()).is_err());
    assert!(Client::new(
        "ws://localhost",
        Auth::new("alice", "secret"),
        Options {
            event_capacity: 0,
            ..options()
        }
    )
    .is_err());
}

#[tokio::test]
async fn outbound_size_limit_does_not_put_oversized_send_on_wire() {
    let (listener, url) = listener().await;
    let server = tokio::spawn(async move {
        let mut peer = authenticated(&listener).await;
        let request = read(&mut peer).await;
        let payload: Value = serde_json::from_slice(
            &STANDARD
                .decode(request["params"]["payload"].as_str().unwrap())
                .unwrap(),
        )
        .unwrap();
        assert_eq!(payload, json!({"ok":true}));
        write(
            &mut peer,
            json!({"id":request["id"],"result":{"messageId":"1","messageSeq":1,"reasonCode":1}}),
        )
        .await;
        let _ = bounded(peer.next()).await;
    });
    let client = client(
        url,
        Options {
            max_message_size: 512,
            ..options()
        },
    );
    client.connect().await.unwrap();
    assert_eq!(
        client
            .send("bob", 1u8, json!({"content":"x".repeat(512)}))
            .await
            .unwrap_err(),
        Error::MessageTooLarge
    );
    client.send("bob", 1u8, json!({"ok":true})).await.unwrap();
    client.disconnect().await;
    bounded(server).await.unwrap();
}

#[tokio::test]
async fn incoming_message_limit_disconnects_without_delivering_oversized_payload() {
    let (listener, url) = listener().await;
    let server = tokio::spawn(async move {
        let mut peer = authenticated(&listener).await;
        write(&mut peer, recv(json!({"content":"x".repeat(1024)}))).await;
        let _ = bounded(peer.next()).await;
    });
    let client = client(
        url,
        Options {
            max_message_size: 512,
            max_reconnect_attempts: 0,
            ..options()
        },
    );
    let mut events = client.subscribe();
    client.connect().await.unwrap();
    bounded(async {
        loop {
            match events.recv().await.unwrap() {
                Event::Message(_) => panic!("oversized message delivered"),
                Event::Disconnect(_) => break,
                _ => {}
            }
        }
    })
    .await;
    client.disconnect().await;
    bounded(server).await.unwrap();
}

#[tokio::test]
async fn explicit_reconnect_reuses_device_id_and_destroy_applies_to_all_clones() {
    let (listener, url) = listener().await;
    let server = tokio::spawn(async move {
        let mut device_id = None;
        for _ in 0..2 {
            let (mut peer, connect) = accept(&listener).await;
            if let Some(id) = &device_id {
                assert_eq!(id, &connect["params"]["deviceId"]);
            } else {
                device_id = Some(connect["params"]["deviceId"].clone());
            }
            write(
                &mut peer,
                json!({"id":connect["id"],"result":{"reasonCode":1}}),
            )
            .await;
            let _ = bounded(peer.next()).await;
        }
    });
    let client = client(url, options());
    let clone = client.clone();
    client.connect().await.unwrap();
    client.disconnect().await;
    clone.connect().await.unwrap();
    client.destroy().await;
    assert_eq!(clone.connect().await.unwrap_err(), Error::Destroyed);
    assert_eq!(
        clone.send("bob", 1u8, json!({})).await.unwrap_err(),
        Error::Destroyed
    );
    bounded(server).await.unwrap();
}
