mod api;
mod host;
mod signal;

use anyhow::{anyhow, Result};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

const STATE_PATH: &str = "/root/.nrd/device.json";

struct Ctx {
    room_token: String,
    sig: signal::Signal,
    sessions: HashMap<String, Arc<host::Host>>,
    meta_by_control: HashMap<String, host::SessionMeta>,
    last_meta: Option<host::SessionMeta>,
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let display = std::env::var("NRD_DISPLAY").unwrap_or_else(|_| ":1".to_string());
    let fps: u32 = std::env::var("NRD_FPS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(15);
    let size = std::env::var("NRD_SIZE").unwrap_or_else(|_| "1920x1080".to_string());

    let st = api::load_state(STATE_PATH)?;
    println!("device {}", st.device_id);
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
        if base.ends_with('/') {
            base
        } else {
            format!("{}/", base)
        }
    );
    println!("signalling {}", signal_url);

    let sig = signal::Signal::connect(&signal_url, &room_token).await?;
    println!("signalling ready");

    let mut ctx = Ctx {
        room_token: room_token.clone(),
        sig,
        sessions: HashMap::new(),
        meta_by_control: HashMap::new(),
        last_meta: None,
    };

    // 1. join the room
    ctx.sig.emit("room_info", json!({"room_token": ctx.room_token})).await?;
    println!("room_info sent");

    // 2. publish our capabilities; without this the device stays offline
    let control_id = uuid_like();
    publish_control(&mut ctx, control_id.as_str(), fps, size).await?;
    println!("control sent, device should be online now");

    // keep the presence alive with engine.io pings
    loop {
        match ctx.sig.next_event().await {
            Ok(Some((name, payload))) => {
                log::info!("event {}", name);
                if let Err(e) = handle(&mut ctx, payload, &display, fps, &size).await {
                    log::warn!("handle: {}", e);
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

/// Build the "control" frame that registers this device as a publisher.
///
/// Shape mirrors what the official controlled client sends:
/// ["control", {app_control_id, app_data{_placeholder}, geo_exit_ip,
///              geo_timezone, streamer_data:"<json string>"}]
async fn publish_control(ctx: &mut Ctx, control_id: &str, fps: u32, size: &str) -> Result<()> {
    let (w, h) = parse_size(size);

    let streamer = json!({
        "control_id": control_id,
        "device_capability": {
            "display_info": [
                {"id": 0, "fps": 60, "type": 0, "hdr": -1}
            ],
            "video_codec_capability": [
                // codec 1 = H264, codec_impl 35 = hardware on the phone side
                {"video_codec": 1, "width": w, "height": h, "chroma_sampling": 1, "bit_depth": 8, "codec_impl": -1},
                {"video_codec": 1, "width": 0, "height": 0, "chroma_sampling": 3, "bit_depth": 8, "codec_impl": -1}
            ],
            "ice_id": ""
        }
    });

    let payload = json!({
        "app_control_id": control_id,
        "app_data": {"_placeholder": true, "num": 0},
        "geo_exit_ip": "",
        "geo_timezone": "Asia/Shanghai",
        "streamer_data": streamer.to_string(),
    });

    // Sent as a binary attachment so it matches the official framing.
    ctx.sig.emit_binary("control", payload).await?;
    let _ = fps;
    Ok(())
}

fn parse_size(size: &str) -> (u32, u32) {
    let mut it = size.split('x');
    let w = it.next().and_then(|v| v.parse().ok()).unwrap_or(1920);
    let h = it.next().and_then(|v| v.parse().ok()).unwrap_or(1080);
    (w, h)
}

fn uuid_like() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let a = (nanos & 0xffffffff) as u32;
    let b = ((nanos >> 32) & 0xffff) as u16;
    let c = ((nanos >> 48) & 0xffff) as u16;
    let rand = std::process::id();
    format!(
        "{:08x}-{:04x}-4{:03x}-a{:03x}-{:012x}",
        a,
        b,
        c & 0xfff,
        (rand as u16) & 0xfff,
        nanos & 0xffffffffffff
    )
}

async fn handle(
    ctx: &mut Ctx,
    payload: Value,
    display: &str,
    fps: u32,
    size: &str,
) -> Result<()> {
    // Session announcement carrying iceServers.
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

    // Offer
    if let Some(sdp) = find_str(&payload, "sdp") {
        if sdp.contains("m=video") && sdp.contains("a=ice-ufrag") {
            log::info!("offer {} bytes", sdp.len());
            return start_session(ctx, &payload, &sdp, display, fps, size).await;
        }
    }

    // Remote candidate
    if let Some(cand) = find_candidate(&payload) {
        let control = find_str(&payload, "app_control_id").unwrap_or_default();
        let chosen = ctx
            .sessions
            .get(&control)
            .or_else(|| ctx.sessions.values().next())
            .cloned();

        if let Some(h) = chosen {
            let mid = find_str(&payload, "sdpMid").unwrap_or_else(|| "0".to_string());
            log::info!("remote candidate {}", truncate(&cand, 90));
            h.add_remote_candidate(&cand, &mid).await.ok();
        } else {
            log::info!("candidate before session, dropped");
        }
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

    let base = ctx
        .meta_by_control
        .get(&control)
        .cloned()
        .or_else(|| ctx.last_meta.clone())
        .ok_or_else(|| anyhow!("no iceServers yet"))?;

    let mut meta = base;
    if let Some(v) = find_str(payload, "app_control_id") {
        meta.app_control_id = v;
    }
    if let Some(v) = find_str(payload, "ice_id") {
        meta.ice_id = v;
    }

    let h = Arc::new(host::Host::new(&meta, display, fps, size).await?);
    let answer = h.answer(sdp).await?;
    ctx.sessions.insert(meta.app_control_id.clone(), h.clone());

    let talk = host::answer_payload(&meta, &answer);
    ctx.sig.emit("soac", talk).await?;
    log::info!("answer sent ({} bytes)", answer.len());

    let h2 = h.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(2)).await;
            let s = h2.pc.ice_connection_state();
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