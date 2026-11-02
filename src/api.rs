use anyhow::{anyhow, Result};
use hmac::{Hmac, Mac};
use serde::Deserialize;
use sha2::Sha256;
use std::collections::BTreeMap;

const API_BASE: &str = "https://api.nrd.nie.163.com";
const SIGN_KEY: &str = "alWiSzXZTLu3WfFnw13uBru3";
const UA: &str = "UURemote/4.45.0 (Windows NT 10.0; Win64; x64)";

#[derive(Debug, Deserialize)]
pub struct State {
    pub client_id: String,
    pub device_id: String,
    pub token: String,
    #[serde(default)]
    pub user_id: String,
    #[serde(default)]
    pub csrf: String,
}

pub fn load_state(path: &str) -> Result<State> {
    let raw = std::fs::read_to_string(path)?;
    Ok(serde_json::from_str(&raw)?)
}

/// Decode a JWT payload without verifying the signature.
pub fn jwt_claims(token: &str) -> Result<serde_json::Value> {
    let payload = token
        .split('.')
        .nth(1)
        .ok_or_else(|| anyhow!("not a jwt"))?;

    use base64::Engine as _;
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(payload)
        .or_else(|_| base64::engine::general_purpose::URL_SAFE.decode(payload))?;

    Ok(serde_json::from_slice(&bytes)?)
}

pub struct Client {
    pub token: String,
    pub client_id: String,
    pub device_id: String,
    pub user_id: String,
    pub csrf: String,
    pub http: reqwest::Client,
}

impl Client {
    pub fn new(st: &State) -> Result<Self> {
        let claims = jwt_claims(&st.token).unwrap_or(serde_json::Value::Null);
        let csrf = claims
            .get("csrf")
            .and_then(|v| v.as_str())
            .map(str::to_owned)
            .unwrap_or_else(|| st.csrf.clone());

        Ok(Self {
            token: st.token.clone(),
            client_id: st.client_id.clone(),
            device_id: st.device_id.clone(),
            user_id: st.user_id.clone(),
            csrf,
            http: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(25))
                .build()?,
        })
    }

    fn params(&self) -> BTreeMap<String, String> {
        let mut p = BTreeMap::new();
        p.insert("x-param-chn".into(), "nochannel".into());
        p.insert("x-param-pkgn".into(), "com.netease.uuremote".into());
        p.insert("x-param-plat".into(), "1".into());
        p.insert("x-param-rel".into(), "1".into());
        p.insert("x-param-vc".into(), "44504318".into());
        p.insert("x-param-vn".into(), "4.45.0.4318".into());
        p.insert("x-param-lang".into(), "zh-CN".into());
        p.insert("x-param-cnt".into(), "CN".into());

        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis())
            .unwrap_or(0);
        p.insert("x-param-ts".into(), ts.to_string());

        if !self.client_id.is_empty() {
            p.insert("x-param-client-id".into(), self.client_id.clone());
        }
        if !self.device_id.is_empty() {
            p.insert("x-param-device-id".into(), self.device_id.clone());
        }
        if !self.user_id.is_empty() {
            p.insert("x-param-user-id".into(), self.user_id.clone());
        }
        p
    }

    /// HMAC-SHA256 over METHOD + PATH + "&".join(sorted(x-param-*)) + BODY
    fn sign(method: &str, path: &str, params: &BTreeMap<String, String>, body: &str) -> String {
        let joined = params
            .iter()
            .filter(|(k, _)| k.starts_with("x-param-") && k.as_str() != "x-param-sign")
            .map(|(k, v)| format!("{}={}", k, v))
            .collect::<Vec<_>>()
            .join("&");

        let raw = format!("{}{}{}{}", method.to_uppercase(), path, joined, body);

        let mut mac = Hmac::<Sha256>::new_from_slice(SIGN_KEY.as_bytes())
            .expect("hmac accepts any key length");
        mac.update(raw.as_bytes());
        hex::encode(mac.finalize().into_bytes())
    }

    pub async fn post(
        &self,
        path: &str,
        payload: serde_json::Value,
    ) -> Result<(u16, serde_json::Value)> {
        let body = serde_json::to_string(&payload)?;
        let params = self.params();
        let sign = Self::sign("POST", path, &params, &body);

        let mut req = self
            .http
            .post(format!("{}{}", API_BASE, path))
            .header("Content-Type", "application/json")
            .header("Accept", "application/json")
            .header("User-Agent", UA)
            .header("x-param-sign", sign)
            .body(body);

        for (k, v) in &params {
            req = req.header(k, v);
        }

        if !self.token.is_empty() {
            req = req.header("Authorization", format!("Bearer {}", self.token));
            if !self.csrf.is_empty() {
                req = req
                    .header(
                        "Cookie",
                        format!(
                            "nrd_access_token={}; nrd_csrf_access_token={}",
                            self.token, self.csrf
                        ),
                    )
                    .header("X-CSRF-TOKEN", &self.csrf);
            }
        }

        let resp = req.send().await?;
        let status = resp.status().as_u16();
        let text = resp.text().await?;

        let doc = serde_json::from_str(&text)
            .unwrap_or_else(|_| serde_json::json!({ "_raw": text }));

        Ok((status, doc))
    }

    pub async fn create_room(&self) -> Result<serde_json::Value> {
        let (status, doc) = self
            .post("/api/v1/room/create", serde_json::json!({}))
            .await?;

        if status != 200 {
            return Err(anyhow!("room/create -> {} {:?}", status, doc));
        }
        doc.get("data")
            .cloned()
            .ok_or_else(|| anyhow!("room/create: missing data"))
    }
}