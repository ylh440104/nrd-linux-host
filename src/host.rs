use anyhow::{anyhow, Result};
use serde_json::{json, Value};
use std::sync::Arc;
use tokio::io::AsyncReadExt;
use tokio::process::Command;
use tokio::sync::mpsc::UnboundedSender;

use webrtc::api::interceptor_registry::register_default_interceptors;
use webrtc::api::media_engine::{MediaEngine, MIME_TYPE_H264};
use webrtc::api::APIBuilder;
use webrtc::ice_transport::ice_candidate::{RTCIceCandidate, RTCIceCandidateInit};
use webrtc::ice_transport::ice_server::RTCIceServer;
use webrtc::interceptor::registry::Registry;
use webrtc::peer_connection::configuration::RTCConfiguration;
use webrtc::peer_connection::peer_connection_state::RTCPeerConnectionState;
use webrtc::peer_connection::sdp::session_description::RTCSessionDescription;
use webrtc::peer_connection::RTCPeerConnection;
use webrtc::rtp_transceiver::rtp_codec::RTCRtpCodecCapability;
use webrtc::track::track_local::track_local_static_sample::TrackLocalStaticSample;
use webrtc::track::track_local::TrackLocal;

/// Parsed view of the "be-controlled" payload.
#[derive(Debug, Default, Clone)]
pub struct SessionMeta {
    pub ice_servers: Vec<RTCIceServerSpec>,
    pub force_relay: bool,
    pub app_control_id: String,
    pub ice_id: String,
    pub client_id: String,
    pub source: String,
}

#[derive(Debug, Clone)]
pub struct RTCIceServerSpec {
    pub urls: String,
    pub username: String,
    pub credential: String,
}

/// Walk an arbitrary json tree looking for the iceServers array.
///
/// The server has sent this payload in several shapes over time (text event,
/// binary attachment, nested inside "streamer_data" as a string), so a
/// structural search is more reliable than a fixed path.
pub fn extract_meta(root: &Value) -> Option<SessionMeta> {
    let mut meta = SessionMeta::default();

    if let Some(v) = find_key(root, "iceServers") {
        if let Some(arr) = v.as_array() {
            for item in arr {
                let single = item.get("urls").and_then(|u| u.as_str());
                if let Some(url) = single {
                    if !url.is_empty() {
                        meta.ice_servers.push(RTCIceServerSpec {
                            urls: url.to_string(),
                            username: str_of(item, "username"),
                            credential: str_of(item, "credential"),
                        });
                    }
                    continue;
                }
                if let Some(list) = item.get("urls").and_then(|u| u.as_array()) {
                    for u in list {
                        if let Some(s) = u.as_str() {
                            meta.ice_servers.push(RTCIceServerSpec {
                                urls: s.to_string(),
                                username: str_of(item, "username"),
                                credential: str_of(item, "credential"),
                            });
                        }
                    }
                }
            }
        }
    }

    if let Some(v) = find_key(root, "force_relay") {
        meta.force_relay = v.as_bool().unwrap_or(false);
    }
    meta.app_control_id = find_key(root, "app_control_id")
        .and_then(|v| v.as_str().map(str::to_owned))
        .unwrap_or_default();
    meta.ice_id = find_key(root, "ice_id")
        .and_then(|v| v.as_str().map(str::to_owned))
        .unwrap_or_default();
    meta.client_id = find_key(root, "client_id")
        .and_then(|v| v.as_str().map(str::to_owned))
        .unwrap_or_default();

    if meta.ice_servers.is_empty() {
        return None;
    }
    Some(meta)
}

fn str_of(v: &Value, key: &str) -> String {
    v.get(key).and_then(|x| x.as_str()).unwrap_or_default().to_string()
}

/// Depth-limited search for a key anywhere in an already-parsed tree.
///
/// Returns an owned copy because some payloads hide json inside a string
/// ("streamer_data"), and that inner tree has a shorter lifetime.
fn find_key(root: &Value, key: &str) -> Option<Value> {
    walk(root, key, 0)
}

fn walk(v: &Value, key: &str, depth: usize) -> Option<Value> {
    if depth > 8 {
        return None;
    }
    match v {
        Value::Object(map) => {
            if let Some(found) = map.get(key) {
                return Some(found.clone());
            }
            for (_, child) in map {
                if let Some(found) = walk(child, key, depth + 1) {
                    return Some(found);
                }
            }
            // Some payloads embed json as a string inside "streamer_data".
            if let Some(s) = map.get("streamer_data").and_then(|x| x.as_str()) {
                if let Ok(inner) = serde_json::from_str::<Value>(s) {
                    if let Some(found) = walk(&inner, key, depth + 1) {
                        return Some(found);
                    }
                }
            }
            None
        }
        Value::Array(items) => {
            for child in items {
                if let Some(found) = walk(child, key, depth + 1) {
                    return Some(found);
                }
            }
            None
        }
        _ => None,
    }
}

pub struct Host {
    pub pc: Arc<RTCPeerConnection>,
    video: Arc<TrackLocalStaticSample>,
}

impl Host {
    pub async fn new(meta: &SessionMeta, display: &str, fps: u32, size: &str) -> Result<Self> {
        let mut media = MediaEngine::default();
        // register_default_codecs already includes H264 baseline and VP8.
        media.register_default_codecs()?;

        let mut registry = Registry::new();
        registry = register_default_interceptors(registry, &mut media)?;

        let api = APIBuilder::new()
            .with_media_engine(media)
            .with_interceptor_registry(registry)
            .build();

        let mut servers = Vec::new();
        for spec in &meta.ice_servers {
            let base = spec.urls.split('?').next().unwrap_or(&spec.urls);
            if !(base.starts_with("turn:") || base.starts_with("turns:") || base.starts_with("stun:")) {
                continue;
            }
            servers.push(RTCIceServer {
                urls: vec![spec.urls.clone()],
                username: spec.username.clone(),
                credential: spec.credential.clone(),
                ..Default::default()
            });
        }
        log::info!("ice servers kept: {}", servers.len());
        for s in &servers {
            log::info!("  {}", s.urls.join(","));
        }

        let mut cfg = RTCConfiguration {
            ice_servers: servers,
            ..Default::default()
        };
        if meta.force_relay {
            log::info!("force_relay set -> policy Relay");
            cfg.ice_transport_policy = webrtc::peer_connection::policy::ice_transport_policy::RTCIceTransportPolicy::Relay;
        }

        let pc = Arc::new(api.new_peer_connection(cfg).await?);

        let video = Arc::new(TrackLocalStaticSample::new(
            RTCRtpCodecCapability {
                mime_type: MIME_TYPE_H264.to_owned(),
                clock_rate: 90000,
                channels: 0,
                sdp_fmtp_line: "level-asymmetry-allowed=1;packetization-mode=1;profile-level-id=42e01f".to_owned(),
                rtcp_feedback: vec![],
            },
            "video".to_owned(),
            "nrd".to_owned(),
        ));

        let track: Arc<dyn TrackLocal + Send + Sync> = video.clone();
        pc.add_track(track).await?;

        {
            let pc2 = pc.clone();
            pc.on_peer_connection_state_change(Box::new(move |s: RTCPeerConnectionState| {
                log::info!("pc state {:?}", s);
                let pc3 = pc2.clone();
                Box::pin(async move {
                    if s == RTCPeerConnectionState::Connected {
                        log::info!("ICE CONNECTED");
                    }
                    let _ = pc3;
                })
            }));
        }

        {
            let pc2 = pc.clone();
            pc.on_ice_connection_state_change(Box::new(
                move |s: webrtc::ice_transport::ice_connection_state::RTCIceConnectionState| {
                    log::info!("ice state {:?}", s);
                    let _ = &pc2;
                    Box::pin(async {})
                },
            ));
        }

        pc.on_ice_candidate(Box::new(move |c: Option<RTCIceCandidate>| {
            Box::pin(async move {
                if let Some(c) = c {
                    let text = c
                        .to_json()
                        .map(|j| j.candidate)
                        .unwrap_or_default();
                    log::info!("local candidate {}", text);
                }
            })
        }));

        Ok(Self { pc, video })
    }

    /// Apply the remote offer and produce our answer.
    pub async fn answer(&self, offer_sdp: &str) -> Result<String> {
        let offer = RTCSessionDescription::offer(offer_sdp.to_owned())?;
        self.pc.set_remote_description(offer).await?;

        let answer = self.pc.create_answer(None).await?;
        self.pc.set_local_description(answer).await?;

        let local = self
            .pc
            .local_description()
            .await
            .ok_or_else(|| anyhow!("no local description"))?;

        let cand_count = local.sdp.matches("a=candidate:").count();
        log::info!("answer ready, inline candidates {}", cand_count);
        Ok(local.sdp)
    }

    pub async fn add_remote_candidate(&self, cand: &str, mid: &str) -> Result<()> {
        let init = RTCIceCandidateInit {
            candidate: cand.to_owned(),
            sdp_mid: Some(mid.to_owned()),
            sdp_mline_index: Some(0),
            username_fragment: None,
        };
        // webrtc 0.11 returns Result here, not a future.
        self.pc.add_ice_candidate(init).await?;
        Ok(())
    }

    /// Pump x11grab through ffmpeg into the H264 track.
    pub async fn spawn_video(&self, display: &str, fps: u32, size: &str) -> Result<()> {
        let video = self.video.clone();
        let display = display.to_string();
        let size = size.to_string();

        tokio::spawn(async move {
            loop {
                match pump_once(video.clone(), &display, fps, &size).await {
                    Ok(()) => log::warn!("ffmpeg exited, restarting"),
                    Err(e) => log::warn!("ffmpeg error: {}", e),
                }
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
        });
        Ok(())
    }
}

async fn pump_once(
    track: Arc<TrackLocalStaticSample>,
    display: &str,
    fps: u32,
    size: &str,
) -> Result<()> {
    let mut child = Command::new("ffmpeg")
        .args([
            "-loglevel", "error",
            "-f", "x11grab",
            "-framerate", &fps.to_string(),
            "-video_size", size,
            "-i", display,
            "-vcodec", "libx264",
            "-preset", "ultrafast",
            "-tune", "zerolatency",
            "-profile:v", "baseline",
            "-pix_fmt", "yuv420p",
            "-b:v", "1500k",
            "-maxrate", "2000k",
            "-bufsize", "2000k",
            "-g", &(fps * 2).to_string(),
            "-f", "h264",
            "pipe:1",
        ])
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true)
        .spawn()?;

    let mut stdout = child.stdout.take().ok_or_else(|| anyhow!("no stdout"))?;
    let duration = std::time::Duration::from_millis((1000 / fps.max(1)) as u64);

    let mut buf: Vec<u8> = Vec::with_capacity(1 << 20);
    let mut chunk = vec![0u8; 64 * 1024];
    let mut frame_count: u64 = 0;

    loop {
        let n = stdout.read(&mut chunk).await?;
        if n == 0 {
            break;
        }
        buf.extend_from_slice(&chunk[..n]);

        // Split Annex-B: emit every complete NAL unit except a trailing partial.
        loop {
            let Some(first) = find_start_code(&buf, 0) else { break };
            let Some(next) = find_start_code(&buf, first + 3) else { break };

            let nal = buf[first..next].to_vec();
            buf.drain(..next);

            if nal.len() > 6 {
                use webrtc::media::Sample;
                track
                    .write_sample(&Sample {
                        data: nal.into(),
                        duration,
                        ..Default::default()
                    })
                    .await?;
                frame_count += 1;
                if frame_count % 60 == 1 {
                    log::info!("sent {} NAL units", frame_count);
                }
            }
        }

        if buf.len() > 4 << 20 {
            // Avoid unbounded growth if we ever fail to find a boundary.
            buf.clear();
        }
    }

    let _ = child.wait().await;
    Ok(())
}

fn find_start_code(buf: &[u8], from: usize) -> Option<usize> {
    let mut i = from;
    while i + 3 <= buf.len() {
        if buf[i] == 0 && buf[i + 1] == 0 {
            if buf[i + 2] == 1 {
                return Some(i);
            }
            if i + 4 <= buf.len() && buf[i + 2] == 0 && buf[i + 3] == 1 {
                return Some(i);
            }
        }
        i += 1;
    }
    None
}

/// Build the "soac" answer payload.
pub fn answer_payload(meta: &SessionMeta, sdp: &str) -> Value {
    json!({
        "app_control_id": meta.app_control_id,
        "ice_id": meta.ice_id,
        "sdp": sdp,
        "type": "answer",
        "ice_network_type": 3,
    })
}

pub fn candidate_payload(meta: &SessionMeta, cand: &str, mid: &str) -> Value {
    json!({
        "app_control_id": meta.app_control_id,
        "ice_id": meta.ice_id,
        "candidate": {
            "candidate": cand,
            "sdpMLineIndex": 0,
            "sdpMid": mid,
        },
        "type": "candidate",
    })
}

/// Forward one outbound soac payload through the signalling channel.
pub fn push_soac(tx: &UnboundedSender<Value>, payload: Value) {
    let _ = tx.send(payload);
}