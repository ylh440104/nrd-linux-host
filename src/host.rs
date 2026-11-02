use anyhow::{anyhow, Result};
use serde_json::{json, Value};
use std::sync::Arc;
use tokio::io::AsyncReadExt;
use tokio::process::Command;

use webrtc::api::interceptor_registry::register_default_interceptors;
use webrtc::api::media_engine::{MediaEngine, MIME_TYPE_H264};
use webrtc::api::APIBuilder;
use webrtc::ice_transport::ice_candidate::{RTCIceCandidate, RTCIceCandidateInit};
use webrtc::ice_transport::ice_credential_type::RTCIceCredentialType;
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
    pub ice_servers: Vec<IceServerSpec>,
    pub force_relay: bool,
    pub app_control_id: String,
    pub ice_id: String,
    pub client_id: String,
}

#[derive(Debug, Clone)]
pub struct IceServerSpec {
    pub urls: String,
    pub username: String,
    pub credential: String,
}

impl IceServerSpec {
    fn is_turn(&self) -> bool {
        let base = self.urls.split('?').next().unwrap_or(&self.urls);
        base.starts_with("turn:") || base.starts_with("turns:")
    }

    fn is_stun(&self) -> bool {
        let base = self.urls.split('?').next().unwrap_or(&self.urls);
        base.starts_with("stun:")
    }
}

/// Walk an arbitrary json tree looking for the iceServers array.
pub fn extract_meta(root: &Value) -> Option<SessionMeta> {
    let mut meta = SessionMeta::default();

    if let Some(v) = find_key(root, "iceServers") {
        if let Some(arr) = v.as_array() {
            for item in arr {
                let single = item.get("urls").and_then(|u| u.as_str());
                if let Some(url) = single {
                    if !url.is_empty() {
                        meta.ice_servers.push(IceServerSpec {
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
                            meta.ice_servers.push(IceServerSpec {
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
    v.get(key)
        .and_then(|x| x.as_str())
        .unwrap_or_default()
        .to_string()
}

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
        media.register_default_codecs()?;

        let mut registry = Registry::new();
        registry = register_default_interceptors(registry, &mut media)?;

        let api = APIBuilder::new()
            .with_media_engine(media)
            .with_interceptor_registry(registry)
            .build();

        let servers = build_ice_servers(meta);

        let mut cfg = RTCConfiguration {
            ice_servers: servers,
            ..Default::default()
        };

        if meta.force_relay {
            log::info!("force_relay -> RTCIceTransportPolicy::Relay");
            cfg.ice_transport_policy =
                webrtc::peer_connection::policy::ice_transport_policy::RTCIceTransportPolicy::Relay;
        }

        let pc = Arc::new(api.new_peer_connection(cfg).await?);

        let video = Arc::new(TrackLocalStaticSample::new(
            RTCRtpCodecCapability {
                mime_type: MIME_TYPE_H264.to_owned(),
                clock_rate: 90000,
                channels: 0,
                sdp_fmtp_line: "level-asymmetry-allowed=1;packetization-mode=1;profile-level-id=42e01f"
                    .to_owned(),
                rtcp_feedback: vec![],
            },
            "video".to_owned(),
            "nrd".to_owned(),
        ));

        let track: Arc<dyn TrackLocal + Send + Sync> = video.clone();
        pc.add_track(track).await?;

        pc.on_peer_connection_state_change(Box::new(move |s: RTCPeerConnectionState| {
            log::info!("pc state {:?}", s);
            Box::pin(async {})
        }));

        pc.on_ice_connection_state_change(Box::new(
            move |s: webrtc::ice_transport::ice_connection_state::RTCIceConnectionState| {
                log::info!("ice state {:?}", s);
                Box::pin(async {})
            },
        ));

        pc.on_ice_candidate(Box::new(move |c: Option<RTCIceCandidate>| {
            Box::pin(async move {
                if let Some(c) = c {
                    let text = c.to_json().map(|j| j.candidate).unwrap_or_default();
                    log::info!("local candidate {}", text);
                }
            })
        }));

        let _ = (display, fps, size);
        Ok(Self { pc, video })
    }

    pub async fn answer(&self, offer_sdp: &str) -> Result<String> {
        let offer = RTCSessionDescription::offer(offer_sdp.to_owned())?;
        self.pc.set_remote_description(offer).await?;

        let answer = self.pc.create_answer(None).await?;
        self.pc.set_local_description(answer).await?;

        // Candidates are gathered asynchronously. Wait for the gatherer to
        // finish so the SDP we hand back actually contains our relay address;
        // otherwise the peer has nothing to send checks to.
        let mut gather_done = self.pc.gathering_complete_promise().await;
        match tokio::time::timeout(std::time::Duration::from_secs(12), gather_done.recv()).await {
            Ok(_) => log::info!("ice gathering complete"),
            Err(_) => log::warn!("ice gathering timed out, using whatever we have"),
        }

        let local = self
            .pc
            .local_description()
            .await
            .ok_or_else(|| anyhow!("no local description"))?;

        let candidates = local.sdp.matches("a=candidate:").count();
        log::info!("answer built, inline candidates {}", candidates);
        Ok(local.sdp)
    }

    pub async fn add_remote_candidate(&self, cand: &str, mid: &str) -> Result<()> {
        let init = RTCIceCandidateInit {
            candidate: cand.to_owned(),
            sdp_mid: Some(mid.to_owned()),
            sdp_mline_index: Some(0),
            username_fragment: None,
        };
        self.pc.add_ice_candidate(init).await?;
        Ok(())
    }

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

/// Split the server list the way webrtc-rs expects it.
///
/// `turn:` / `turns:` entries are only accepted together with a username and
/// credential; a turn URL without them makes the whole configuration invalid.
/// `stun:` entries must NOT carry credentials.
fn build_ice_servers(meta: &SessionMeta) -> Vec<RTCIceServer> {
    let mut out = Vec::new();
    let mut skipped = 0usize;

    for spec in &meta.ice_servers {
        if spec.is_turn() {
            if spec.username.is_empty() || spec.credential.is_empty() {
                log::warn!("turn without credentials, skipped: {}", spec.urls);
                skipped += 1;
                continue;
            }
            log::info!(
                "turn {} user_len={} cred_len={}",
                spec.urls,
                spec.username.len(),
                spec.credential.len()
            );
            out.push(RTCIceServer {
                urls: vec![spec.urls.clone()],
                username: spec.username.clone(),
                credential: spec.credential.clone(),
                // Default is Unspecified, which webrtc-rs rejects for turn.
                credential_type: RTCIceCredentialType::Password,
            });
        } else if spec.is_stun() {
            log::info!("stun {}", spec.urls);
            out.push(RTCIceServer {
                urls: vec![spec.urls.clone()],
                username: String::new(),
                credential: String::new(),
                credential_type: RTCIceCredentialType::Unspecified,
            });
        } else {
            log::warn!("unknown scheme, skipped: {}", spec.urls);
            skipped += 1;
        }
    }

    log::info!("ice servers: kept {} skipped {}", out.len(), skipped);
    out
}

async fn pump_once(
    track: Arc<TrackLocalStaticSample>,
    display: &str,
    fps: u32,
    size: &str,
) -> Result<()> {
    let mut child = Command::new("ffmpeg")
        .args([
            "-loglevel",
            "error",
            "-f",
            "x11grab",
            "-framerate",
            &fps.to_string(),
            "-video_size",
            size,
            "-i",
            display,
            "-vcodec",
            "libx264",
            "-preset",
            "ultrafast",
            "-tune",
            "zerolatency",
            "-profile:v",
            "baseline",
            "-pix_fmt",
            "yuv420p",
            "-b:v",
            "1500k",
            "-maxrate",
            "2000k",
            "-bufsize",
            "2000k",
            "-g",
            &(fps * 2).to_string(),
            "-f",
            "h264",
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
    let mut frames: u64 = 0;

    loop {
        let n = stdout.read(&mut chunk).await?;
        if n == 0 {
            break;
        }
        buf.extend_from_slice(&chunk[..n]);

        loop {
            let Some(first) = find_start_code(&buf, 0) else { break };
            let Some(next) = find_start_code(&buf, first + 3) else {
                break;
            };

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
                frames += 1;
                if frames % 60 == 1 {
                    log::info!("sent {} NAL units", frames);
                }
            }
        }

        if buf.len() > 4 << 20 {
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

pub fn answer_payload(meta: &SessionMeta, sdp: &str) -> Value {
    json!({
        "app_control_id": meta.app_control_id,
        "ice_id": meta.ice_id,
        "sdp": sdp,
        "type": "answer",
        "ice_network_type": 3,
    })
}