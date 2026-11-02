mod api;
mod host;
mod signal;

use anyhow::{anyhow, Result};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

const STATE_PATH: &str = "/root/.nrd/device.json";

struct Ctx {
    client: api::Client,
    room_token: String,
    signal_url: String,
    sig: signal::Signal,
    /// Session keyed by app_control_id so late candidates find their session.
    sessions: HashMap<String, Arc<host::Host>>,
    meta_by_control: HashMap<String, host::SessionMeta>,
    last_meta: Option<host::SessionMeta>,
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::Builder::from_env(
        env_logger::Env::default().default_filter_or("info"),
    )
    .init();

    let display = std::env::var("NRD_DISPLAY").unwrap_or_else(|_| ":1".to_string());
    let fps: u32 = std::env::var("NRD_FPS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(15);
    let size = std::env::var("NRD_SIZE").unwrap_or_else(|_| "1920x1080".to_string());

    let st = api::load_state(STATE_PATH)?;
    println!("device {}  token {} bytes", st.device_id, st.token.len());
    println!("desktop {} {}@{}", display, size, fps);

    let client = api::Client::new(&st)?;

    let room = client.create_room().await?;
    let room_token = room
        .get("token")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow!("room/create: no token"))?
        .to_string();
    println!("room token {} bytes", room_token.len());

    let mut servers: Vec<String> = Vec::new();
    for key in ["signaling_list", "sig_list", "servers"] {
        if let Some(list) = room.get(key).and_then(|v| v.as_array()) {
            for item in list {
                if let Some(s) = item.as_str() {
                    servers.push(s.to_string());
                } else if let Some(s) = item.get("url").and_then(|v| v.as_str()) {
                    servers.push(s.to_string());
                }
            }
        }
    }
    let base = servers
        .first()
        .cloned()
        .unwrap_or_else(|| "wss://sig-3303-a.nrd.nie.163.com/".to_string());
    let signal_url = format!(
        "{}socket.io/?transport=websocket&EIO=4",
        if base.ends_with('/') { base } else { format!("{}/", base) }
    );
    println!("signalling {}", signal_url);

    let sig = signal::Signal::connect(&signal_url, &room_token).await?;
    println!("signalling ready, waiting for controller ...");

    let mut ctx = Ctx {
        client,
        room_token,
        signal_url,
        sig,
        sessions: HashMap::new(),
        meta_by_control: HashMap::new(),
        last_meta: None,
    };

    // Announce the room. The server expects room_info with the room token so
    // the device shows up as online.
    ctx.sig
        .emit("room_info", json!({ "room_token": ctx.room_token }))
        .await?;

    loop {
        match ctx.sig.next_event().await {
            Ok(Some((name, payload))) => {
                if let Err(e) = handle(&mut ctx, &name, payload, &display, fps, &size).await {
                    log::warn!("handle {}: {}", name, e);
                }
            }
            Ok(None) => {
                log::warn!("signalling closed");
                break;
            }
            Err(e) => {
                log::warn!("signalling error: {}", e);
                break;
            }
        }
    }
    Ok(())
}

async fn handle(
    ctx: &mut Ctx,
    name: &str,
    payload: Value,
    display: &str,
    fps: u32,
    size: &str,
) -> Result<()> {
    if name != "bmsg" && name != "push" && name != "message" {
        log::debug!("event {}", name);
    }

    // Any payload carrying iceServers is a session announcement.
    if let Some(meta) = host::extract_meta(&payload) {
        log::info!(
            "session meta: iceServers {} force_relay {} control {}",
            meta.ice_servers.len(),
            meta.force_relay,
            meta.app_control_id
        );
        for s in &meta.ice_servers {
            log::info!("  {}", s.urls);
        }
        if !meta.app_control_id.is_empty() {
            ctx.meta_by_control
                .insert(meta.app_control_id.clone(), meta.clone());
        }
        ctx.last_meta = Some(meta);
        return Ok(());
    }

    // Offer arrives as {"sdp": ..., "app_control_id": ..., "ice_id": ...}
    if let Some(sdp) = find_str(&payload, "sdp") {
        if sdp.contains("m=video") && sdp.contains("a=ice-ufrag") {
            log::info!("offer {} bytes", sdp.len());
            return start_session(ctx, &payload, &sdp, display, fps, size).await;
        }
    }

    // Remote candidate
    if let Some(cand) = find_candidate(&payload) {
        let control = find_str(&payload, "app_control_id").unwrap_or_default();
        let host = ctx
            .sessions
            .get(&control)
            .or_else(|| ctx.sessions.values().next())
            .cloned();

        match host {
            Some(h) => {
                let mid = find_str(&payload, "sdpMid").unwrap_or_else(|| "0".to_string());
                log::info!("remote candidate {}", truncate(&cand, 90));
                h.add_remote_candidate(&cand, &mid).await.ok();
            }
            None => log::info!("candidate before session, dropped"),
        }
        return Ok(());
    }

    Ok(())
}

async fn start_session(
    ctx: &mut Ctx,
    payload: &Value,
    sdp: &str,
    display: &str,
    fps: u32,
    size: &str,
) -> Result<()> {
    let control = find_str(payload, "app_control_id").unwrap_or_default();

    let meta = ctx
        .meta_by_control
        .get(&control)
        .cloned()
        .or_else(|| ctx.last_meta.clone())
        .ok_or_else(|| anyhow!("no iceServers yet, cannot answer"))?;

    let mut meta = meta;
    // The offer carries the authoritative ids for this session.
    if let Some(v) = find_str(payload, "app_control_id") {
        meta.app_control_id = v;
    }
    if let Some(v) = find_str(payload, "ice_id") {
        meta.ice_id = v;
    }

    let h = host::Host::new(&meta, display, fps, size).await?;
    let h = Arc::new(h);

    let answer = h.answer(sdp).await?;
    ctx.sessions.insert(meta.app_control_id.clone(), h.clone());

    let talk = host::answer_payload(&meta, &answer);
    ctx.sig.emit("soac", talk).await?;
    log::info!("answer sent ({} bytes)", answer.len());

    // A tiny probe so we can watch the ICE state from the log.
    let h2 = h.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(2)).await;
            let s = h2.pc.ice_connection_state().await;
            log::info!("probe: ice={:?}", s);
            if matches!(
                s,
                webrtc::ice_transport::ice_connection_state::RTCIceConnectionState::Connected
                    | webrtc::ice_transport::ice_connection_state::RTCIceConnectionState::Completed
                    | webrtc::ice_transport::ice_connection_state::RTCIceConnectionState::Failed
                    | webrtc::ice_transport::ice_connection_state::RTCIceConnectionState::Closed
            ) {
                break;
            }
        }
    });

    h.spawn_video(display, fps, size).await?;
    Ok(())
}

fn find_str(v: &Value, key: &str) -> Option<String> {
    match v {
        Value::Object(map) => {
            if let Some(x) = map.get(key).and_then(|x| x.as_str()) {
                return Some(x.to_string());
            }
            for (_, child) in map {
                if let Some(found) = find_str(child, key) {
                    return Some(found);
                }
            }
            None
        }
        Value::Array(items) => items.iter().find_map(|c| find_str(c, key)),
        _ => None,
    }
}

fn find_candidate(v: &Value) -> Option<String> {
    if let Some(c) = v.get("candidate") {
        if let Some(s) = c.as_str() {
            return Some(s.to_string());
        }
        if let Some(s) = c.get("candidate").and_then(|x| x.as_str()) {
            return Some(s.to_string());
        }
    }
    match v {
        Value::Object(map) => {
            for (_, child) in map {
                if let Some(found) = find_candidate(child) {
                    return Some(found);
                }
            }
            None
        }
        Value::Array(items) => items.iter().find_map(find_candidate),
        _ => None,
    }
}

fn truncate(s: &str, n: usize) -> String {
    if s.len() <= n {
        s.to_string()
    } else {
        format!("{}...", &s[..n])
    }
}