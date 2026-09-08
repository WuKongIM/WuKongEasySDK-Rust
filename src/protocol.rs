use crate::{Auth, CustomEvent, Error, RecvMessage, SendOptions};
use base64::{engine::general_purpose::STANDARD, Engine};
use serde::de::DeserializeOwned;
use serde_json::{json, Value};
use std::time::{SystemTime, UNIX_EPOCH};

pub(crate) fn request(method: &str, params: Value, id: &str) -> Value {
    json!({"jsonrpc":"2.0", "method":method, "params":params, "id":id})
}

pub(crate) fn connect(auth: &Auth, id: &str) -> Value {
    request(
        "connect",
        json!({
            "uid":auth.uid, "token":auth.token, "deviceId":auth.device_id,
            "deviceFlag":auth.device_flag as u8,
            "clientTimestamp":SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64
        }),
        id,
    )
}

pub(crate) fn send(
    channel: &str,
    kind: u8,
    payload: Value,
    options: SendOptions,
) -> Result<Value, Error> {
    if channel.trim().is_empty()
        || kind == 0
        || !payload.is_object() && !payload.is_array()
        || options
            .client_msg_no
            .as_ref()
            .is_some_and(|s| s.trim().is_empty())
    {
        return Err(Error::InvalidInput(
            "send requires a channel, nonzero type and JSON object/array",
        ));
    }
    let mut params = json!({"channelId":channel, "channelType":kind,
        "payload":STANDARD.encode(serde_json::to_vec(&payload).map_err(|_| Error::Protocol("payload encoding"))?),
        "clientMsgNo":options.client_msg_no.unwrap_or_else(|| uuid::Uuid::new_v4().to_string()),
        "header":options.header});
    if let Some(setting) = options.setting {
        params["setting"] = json!(setting);
    }
    if let Some(topic) = options.topic {
        params["topic"] = json!(topic);
    }
    Ok(params)
}

/// Normalize only protocol metadata, never keys inside application payload/data.
/// A server may emit both spellings in the same response; camelCase takes precedence.
fn normalize(value: &mut Value) {
    if let Some(object) = value.as_object_mut() {
        for (snake, camel) in [
            ("reason_code", "reasonCode"),
            ("server_key", "serverKey"),
            ("time_diff", "timeDiff"),
            ("server_version", "serverVersion"),
            ("node_id", "nodeId"),
            ("message_id", "messageId"),
            ("message_seq", "messageSeq"),
            ("channel_id", "channelId"),
            ("channel_type", "channelType"),
            ("from_uid", "fromUid"),
            ("client_msg_no", "clientMsgNo"),
            ("no_persist", "noPersist"),
            ("red_dot", "redDot"),
            ("sync_once", "syncOnce"),
        ] {
            if let Some(v) = object.remove(snake) {
                object.entry(camel).or_insert(v);
            }
        }
        if let Some(id) = object.get_mut("messageId") {
            if let Some(n) = id.as_u64() {
                *id = Value::String(n.to_string());
            }
        }
        if let Some(header) = object.get_mut("header") {
            normalize(header);
        }
    }
}
pub(crate) fn decode<T: DeserializeOwned>(mut value: Value) -> Result<T, Error> {
    normalize(&mut value);
    serde_json::from_value(value).map_err(|_| Error::Protocol("invalid fields"))
}

pub(crate) fn response(value: &Value) -> Result<Value, Error> {
    match (value.get("result"), value.get("error")) {
        (Some(result), None) => {
            if let Some(code) = result
                .get("reasonCode")
                .or_else(|| result.get("reason_code"))
            {
                let code = code
                    .as_i64()
                    .ok_or(Error::Protocol("invalid reason code"))?;
                if code != 1 {
                    return Err(Error::Server { code });
                }
            }
            Ok(result.clone())
        }
        (None, Some(error)) => Err(Error::Server {
            code: error
                .get("code")
                .and_then(Value::as_i64)
                .ok_or(Error::Protocol("invalid RPC error"))?,
        }),
        _ => Err(Error::Protocol("expected exactly one result or error")),
    }
}

pub(crate) fn recv(value: Value) -> Result<RecvMessage, Error> {
    let mut message: RecvMessage = decode(value)?;
    if message.message_id.is_empty() || message.channel_id.is_empty() {
        return Err(Error::Protocol("missing message identity"));
    }
    if let Some(encoded) = message.payload.as_str() {
        if let Ok(bytes) = STANDARD.decode(encoded) {
            if let Ok(json) = serde_json::from_slice(&bytes) {
                message.payload = json;
            }
        }
    }
    Ok(message)
}

pub(crate) fn event(value: Value) -> Result<CustomEvent, Error> {
    if value.get("data").is_none() {
        return Err(Error::Protocol("missing event data"));
    }
    let mut event: CustomEvent = decode(value)?;
    if event.id.is_empty() || event.event_type.is_empty() {
        return Err(Error::Protocol("missing event identity"));
    }
    if let Some(text) = event.data.as_str() {
        if let Ok(json) = serde_json::from_str(text) {
            event.data = json;
        }
    }
    Ok(event)
}

pub(crate) fn parse(text: &str) -> Result<Value, Error> {
    let value: Value = serde_json::from_str(text).map_err(|_| Error::Protocol("invalid JSON"))?;
    if !value.is_object() || value.get("jsonrpc").is_some_and(|v| v != "2.0") {
        return Err(Error::Protocol("expected JSON-RPC 2.0 object"));
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ConnectResult, SendResult};
    #[test]
    fn duplicate_aliases_and_large_ids_preserve_wire_values() {
        let result: SendResult = decode(json!({"messageId":"18446744073709551615", "message_id":"wrong", "messageSeq":18446744073709551615_u64, "message_seq":1,"reasonCode":1,"reason_code":1})).unwrap();
        assert_eq!(result.message_seq, u64::MAX);
        assert_eq!(result.message_id, u64::MAX.to_string());
        let conn: ConnectResult = decode(json!({"reason_code":1,"node_id":9})).unwrap();
        assert_eq!(conn.node_id, 9);
    }
    #[test]
    fn unicode_payload_and_explicit_header_are_preserved() {
        let mut options = SendOptions::default();
        options.header.red_dot = false;
        let payload = json!({"content":"你好 🦀", "message_id":"business-owned"});
        let params = send("bob", 1, payload.clone(), options).unwrap();
        assert_eq!(params["header"]["redDot"], false);
        let decoded: Value = serde_json::from_slice(
            &STANDARD
                .decode(params["payload"].as_str().unwrap())
                .unwrap(),
        )
        .unwrap();
        assert_eq!(decoded, payload);
    }
    #[test]
    fn validates_envelopes_and_keeps_errors_redacted() {
        assert!(parse("[]").is_err());
        assert!(parse(r#"{"jsonrpc":"1.0"}"#).is_err());
        assert_eq!(response(&json!({"result":null})), Ok(Value::Null));
        assert!(response(&json!({"result":null,"error":{"code":1}})).is_err());
        assert_eq!(
            response(&json!({"result":{"reasonCode":2}})),
            Err(Error::Server { code: 2 })
        );
        let error =
            response(&json!({"error":{"code":128,"message":"secret","data":"token"}})).unwrap_err();
        assert!(!format!("{error:?}").contains("secret"));
        assert!(!format!("{:?}", Auth::new("secret", "secret")).contains("secret"));
    }
    #[test]
    fn events_parse_json_text_but_allow_non_json_data() {
        assert_eq!(
            event(json!({"id":"e","type":"t","timestamp":1,"data":"{\"ok\":true}"}))
                .unwrap()
                .data,
            json!({"ok":true})
        );
        assert_eq!(
            event(json!({"id":"e","type":"t","timestamp":1,"data":"text"}))
                .unwrap()
                .data,
            "text"
        );
        assert!(event(json!({"id":"e","type":"t","timestamp":1})).is_err());
    }
}
