//! Discord stream keep-alive - edisi Rust, satu akun.
//!
//! Konfigurasi:
//!   ENV  TOKEN       -> token akun (wajib, jangan simpan di file/repo)
//!   ENV  ROOM_INPUT  -> (opsional) ID channel / link channel
//!   FILE stream_state.json -> {"room_input": "...", "self_mute": true,
//!                              "self_deaf": true, "self_video": false}

mod util;

use std::{
    env, fs,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use base64::{engine::general_purpose::STANDARD as B64, Engine as _};
use flate2::{Decompress, FlushDecompress};
use futures_util::{stream::SplitSink, SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::{json, value::RawValue, Value};
use tokio::{
    net::TcpStream,
    sync::{
        mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
        watch,
    },
    task::JoinHandle,
    time::{sleep, sleep_until, timeout, Instant},
};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{client::IntoClientRequest, http::HeaderValue, Message},
    MaybeTlsStream, WebSocketStream,
};
use util::{classify_room, format_duration, next_badge, rejoin_delay, RoomRef};

const SERVER_INVITE: &str = "https://discord.gg/cjXbGqVCzq";
const STATE_FILE: &str = "stream_state.json";
const DISCORD_API: &str = "https://discord.com/api/v10";
const GATEWAY_QUERY: &str = "/?v=10&encoding=json&compress=zlib-stream";
const GATEWAY_URL: &str = "wss://gateway.discord.gg/?v=10&encoding=json&compress=zlib-stream";
const USER_AGENT: &str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 \
    (KHTML, like Gecko) discord/1.0.9168 Chrome/128.0.6613.186 Electron/32.2.2 Safari/537.36";

/// Un-pause stream + simpan state + log, tiap 30 menit.
const REFRESH_EVERY: Duration = Duration::from_secs(1800);
/// Tidak ada data sama sekali selama ini -> koneksi dianggap mati.
const RECV_TIMEOUT: Duration = Duration::from_secs(120);
const GATEWAY_RETRY_MIN: Duration = Duration::from_secs(3);
const RETRY_MIN: Duration = Duration::from_secs(5);
const RETRY_MAX: Duration = Duration::from_secs(120);
/// Close code gateway yang tidak boleh di-retry (token salah dsb).
const FATAL_GATEWAY_CODES: [u16; 6] = [4004, 4010, 4011, 4012, 4013, 4014];
/// Close code voice: token/sesi voice tidak berlaku lagi -> join ulang.
const VOICE_FATAL_CODES: [u16; 4] = [4004, 4006, 4009, 4014];

type Res<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
type Ws = WebSocketStream<MaybeTlsStream<TcpStream>>;
type WsSink = SplitSink<Ws, Message>;

// =====================================================================
// Helper umum
// =====================================================================
fn client_props() -> Value {
    json!({
        "os": "Windows",
        "browser": "Discord Client",
        "release_channel": "stable",
        "client_version": "1.0.9168",
        "os_version": "10.0.22631",
        "os_arch": "x64",
        "app_arch": "x64",
        "system_locale": "en-US",
        "browser_user_agent": USER_AGENT,
        "browser_version": "32.2.2",
        "client_build_number": 343349,
        "native_build_number": 53915,
        "client_event_source": null
    })
}

fn super_properties() -> String {
    B64.encode(client_props().to_string())
}

fn unix_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

async fn sleep_until_opt(t: Option<Instant>) {
    match t {
        Some(t) => sleep_until(t).await,
        None => std::future::pending::<()>().await,
    }
}

async fn wait_shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        match signal(SignalKind::terminate()) {
            Ok(mut term) => {
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => {}
                    _ = term.recv() => {}
                }
            }
            Err(_) => {
                let _ = tokio::signal::ctrl_c().await;
            }
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

async fn ws_connect(url: &str) -> Res<Ws> {
    let mut req = url.into_client_request()?;
    let headers = req.headers_mut();
    headers.insert("User-Agent", HeaderValue::from_static(USER_AGENT));
    headers.insert("Origin", HeaderValue::from_static("https://discord.com"));
    let (ws, _) = timeout(Duration::from_secs(20), connect_async(req)).await??;
    Ok(ws)
}

async fn send_json(sink: &mut WsSink, v: Value) -> Res<()> {
    sink.send(Message::Text(v.to_string())).await?;
    Ok(())
}

/// Dekompres satu pesan zlib-stream (konteks dipertahankan antar pesan).
fn inflate(d: &mut Decompress, input: &[u8], out: &mut Vec<u8>) -> Res<()> {
    out.clear();
    let start = d.total_in();
    loop {
        let consumed = (d.total_in() - start) as usize;
        let (in0, out0) = (d.total_in(), d.total_out());
        out.reserve(16 * 1024);
        d.decompress_vec(&input[consumed..], out, FlushDecompress::Sync)?;
        let consumed = (d.total_in() - start) as usize;
        if consumed >= input.len() && out.len() < out.capacity() {
            return Ok(());
        }
        if d.total_in() == in0 && d.total_out() == out0 {
            return Err("zlib: tidak ada kemajuan".into());
        }
    }
}

// =====================================================================
// State file
// =====================================================================
#[derive(Serialize, Deserialize)]
#[serde(default)]
struct State {
    room_input: String,
    self_mute: bool,
    self_deaf: bool,
    self_video: bool,
    total_streamed_seconds: u64,
}

impl Default for State {
    fn default() -> Self {
        State {
            room_input: String::new(),
            self_mute: true,
            self_deaf: true,
            self_video: false,
            total_streamed_seconds: 0,
        }
    }
}

fn load_state() -> State {
    match fs::read_to_string(STATE_FILE) {
        Ok(text) => serde_json::from_str(&text).unwrap_or_else(|e| {
            eprintln!("[!] Gagal membaca {STATE_FILE}: {e}");
            State::default()
        }),
        Err(_) => State::default(),
    }
}

fn save_state(state: &State) {
    if let Ok(text) = serde_json::to_string_pretty(state) {
        let _ = fs::write(STATE_FILE, text);
    }
}

// =====================================================================
// Discord REST (hanya untuk validasi token + resolve channel)
// =====================================================================
enum ApiError {
    Http(u16),
    Other(String),
}

fn api_get(endpoint: &str, token: &str) -> Result<Value, ApiError> {
    let resp = ureq::get(&format!("{DISCORD_API}{endpoint}"))
        .set("Authorization", token.trim())
        .set("User-Agent", USER_AGENT)
        .set("X-Super-Properties", &super_properties())
        .timeout(Duration::from_secs(15))
        .call();
    match resp {
        Ok(r) => r
            .into_json::<Value>()
            .map_err(|e| ApiError::Other(e.to_string())),
        Err(ureq::Error::Status(code, _)) => Err(ApiError::Http(code)),
        Err(e) => Err(ApiError::Other(e.to_string())),
    }
}

struct Target {
    guild_id: Option<String>,
    channel_id: String,
    name: String,
}

fn resolve_room(raw: &str, token: &str) -> Result<Target, String> {
    match classify_room(raw)? {
        RoomRef::Url {
            guild_id,
            channel_id,
        } => {
            let mut guild = guild_id;
            let mut name = format!("Channel {channel_id}");
            if let Ok(info) = api_get(&format!("/channels/{channel_id}"), token) {
                name = info["name"]
                    .as_str()
                    .map(str::to_string)
                    .unwrap_or_else(|| format!("DM/Call ({channel_id})"));
                if let Some(g) = info["guild_id"].as_str() {
                    guild = Some(g.to_string());
                }
            }
            Ok(Target {
                guild_id: guild,
                channel_id,
                name,
            })
        }
        RoomRef::Pair {
            guild_id,
            channel_id,
        } => Ok(Target {
            guild_id: Some(guild_id),
            name: format!("Channel {channel_id}"),
            channel_id,
        }),
        RoomRef::Single(channel_id) => match api_get(&format!("/channels/{channel_id}"), token) {
            Ok(info) => Ok(Target {
                guild_id: info["guild_id"].as_str().map(str::to_string),
                name: info["name"]
                    .as_str()
                    .map(str::to_string)
                    .unwrap_or_else(|| format!("Private Call ({channel_id})")),
                channel_id,
            }),
            Err(ApiError::Http(401)) => Err("Token tidak valid (401 Unauthorized).".into()),
            Err(ApiError::Http(403)) => Err("Akses ditolak ke channel ini (403 Forbidden).".into()),
            Err(ApiError::Http(404)) => {
                Err("Channel tidak ditemukan (404). Cek ID dan pastikan akun ada di server.".into())
            }
            Err(ApiError::Http(c)) => Err(format!("Gagal mengambil info channel (HTTP {c}).")),
            Err(ApiError::Other(e)) => Err(format!("Gagal menghubungi Discord: {e}")),
        },
    }
}

// =====================================================================
// Voice / Stream RTC WebSocket keep-alive (task terpisah, auto-retry)
// =====================================================================
struct RtcParams {
    endpoint: String,
    server_id: String,
    channel_id: String,
    user_id: String,
    session_id: String,
    token: String,
    is_stream: bool,
}

/// Menghentikan task saat di-drop.
struct RtcTask(JoinHandle<()>);

impl Drop for RtcTask {
    fn drop(&mut self) {
        self.0.abort();
    }
}

fn spawn_rtc(p: RtcParams, fatal_tx: UnboundedSender<()>) -> RtcTask {
    RtcTask(tokio::spawn(rtc_run(p, fatal_tx)))
}

async fn rtc_run(p: RtcParams, fatal_tx: UnboundedSender<()>) {
    let mut url = p.endpoint.clone();
    if !(url.starts_with("wss://") || url.starts_with("ws://")) {
        url = format!("wss://{url}");
    }
    if !url.contains("?v=") {
        url = format!("{}/?v=8", url.trim_end_matches('/'));
    }

    let mut backoff = RETRY_MIN;
    loop {
        let mut ready = false;
        if let Ok(true) = rtc_session(&p, &url, &mut ready).await {
            // Token/sesi voice hangus: minta gateway join ulang.
            let _ = fatal_tx.send(());
            return;
        }
        if ready {
            backoff = RETRY_MIN;
        }
        sleep(backoff).await;
        backoff = (backoff * 2).min(RETRY_MAX);
    }
}

/// Ok(true) = ditutup dengan kode fatal; selain itu -> retry.
async fn rtc_session(p: &RtcParams, url: &str, ready: &mut bool) -> Res<bool> {
    let ws = ws_connect(url).await?;
    let (mut sink, mut stream) = ws.split();

    let kind = if p.is_stream { "screen" } else { "video" };
    send_json(
        &mut sink,
        json!({
            "op": 0,
            "d": {
                "server_id": p.server_id,
                "channel_id": p.channel_id,
                "user_id": p.user_id,
                "session_id": p.session_id,
                "token": p.token,
                "video": true,
                "streams": [{"type": kind, "rid": "100", "quality": 100}]
            }
        }),
    )
    .await?;

    let mut hb: Option<Duration> = None;
    let mut next_hb: Option<Instant> = None;
    let mut last_rx = Instant::now();

    loop {
        tokio::select! {
            _ = sleep_until_opt(next_hb) => {
                if last_rx.elapsed() > RECV_TIMEOUT {
                    return Err("voice: tidak ada data (zombie)".into());
                }
                send_json(&mut sink, json!({
                    "op": 3,
                    "d": {"t": unix_millis(), "seq_ack": -1}
                })).await?;
                next_hb = hb.map(|i| Instant::now() + i);
            }
            item = stream.next() => {
                let msg = match item {
                    Some(Ok(m)) => m,
                    Some(Err(e)) => return Err(e.into()),
                    None => return Ok(false),
                };
                last_rx = Instant::now();
                match msg {
                    Message::Close(frame) => {
                        let code = frame.map(|f| u16::from(f.code)).unwrap_or(1000);
                        return Ok(VOICE_FATAL_CODES.contains(&code));
                    }
                    Message::Text(text) => {
                        let Ok(v) = serde_json::from_str::<Value>(&text) else { continue };
                        match v["op"].as_u64() {
                            Some(8) => {
                                let ms = v["d"]["heartbeat_interval"].as_f64().unwrap_or(15000.0);
                                let i = Duration::from_millis(ms as u64);
                                hb = Some(i);
                                next_hb = Some(Instant::now() + i.mul_f64(rand::random::<f64>()));
                            }
                            Some(2) => {
                                *ready = true;
                                let (select, streams) = voice_ready_payloads(&v["d"]);
                                send_json(&mut sink, select).await?;
                                send_json(&mut sink, streams).await?;
                            }
                            _ => {}
                        }
                    }
                    _ => {}
                }
            }
        }
    }
}

fn voice_ready_payloads(d: &Value) -> (Value, Value) {
    let modes: Vec<&str> = d["modes"]
        .as_array()
        .map(|a| a.iter().filter_map(|m| m.as_str()).collect())
        .unwrap_or_default();
    let mode = if modes.contains(&"aead_aes256_gcm_rtpsize") {
        "aead_aes256_gcm_rtpsize"
    } else {
        modes
            .first()
            .copied()
            .unwrap_or("aead_xchacha20_poly1305_rtpsize")
    };
    let ip = d["ip"].as_str().unwrap_or("127.0.0.1");
    let port = d["port"].as_u64().unwrap_or(1337);

    let select = json!({
        "op": 1,
        "d": {
            "protocol": "udp",
            "data": {"address": ip, "port": port, "mode": mode},
            "codecs": [
                {"name": "opus", "type": "audio", "priority": 1000, "payload_type": 120},
                {
                    "name": "H264", "type": "video", "priority": 1000,
                    "payload_type": 101, "rtx_payload_type": 102,
                    "encode": true, "decode": true
                }
            ],
            "rtc_connection_id": uuid::Uuid::new_v4().to_string()
        }
    });

    let ssrc = d["ssrc"].as_u64().unwrap_or(1);
    let first = d["streams"].as_array().and_then(|a| a.first());
    let video_ssrc = first.and_then(|s| s["ssrc"].as_u64()).unwrap_or(ssrc + 1);
    let rtx_ssrc = first
        .and_then(|s| s["rtx_ssrc"].as_u64())
        .unwrap_or(video_ssrc + 1);

    let streams = json!({
        "op": 12,
        "d": {
            "audio_ssrc": ssrc,
            "video_ssrc": video_ssrc,
            "rtx_ssrc": rtx_ssrc,
            "streams": [{
                "type": "video",
                "rid": "100",
                "ssrc": video_ssrc,
                "active": true,
                "quality": 100,
                "rtx_ssrc": rtx_ssrc,
                "max_bitrate": 2_500_000,
                "max_framerate": 30,
                "max_resolution": {"type": "fixed", "width": 1280, "height": 720}
            }]
        }
    });
    (select, streams)
}

// =====================================================================
// Payload gateway (diparse hemat: READY besar tidak dijadikan Value)
// =====================================================================
#[derive(Deserialize)]
struct Envelope<'a> {
    op: u8,
    #[serde(default)]
    s: Option<u64>,
    #[serde(default, borrow)]
    t: Option<&'a str>,
    #[serde(default, borrow)]
    d: Option<&'a RawValue>,
}

#[derive(Deserialize)]
struct Hello {
    #[serde(default = "default_hb")]
    heartbeat_interval: f64,
}

fn default_hb() -> f64 {
    41250.0
}

#[derive(Deserialize)]
struct Ready {
    user: ReadyUser,
    session_id: String,
    #[serde(default)]
    resume_gateway_url: Option<String>,
}

#[derive(Deserialize)]
struct ReadyUser {
    id: String,
    #[serde(default)]
    username: Option<String>,
}

#[derive(Deserialize)]
struct VoiceStateUpdate {
    user_id: String,
    #[serde(default)]
    channel_id: Option<String>,
    #[serde(default)]
    session_id: Option<String>,
}

#[derive(Deserialize)]
struct ServerUpdate {
    #[serde(default)]
    endpoint: Option<String>,
    #[serde(default)]
    token: Option<String>,
    #[serde(default)]
    guild_id: Option<String>,
    #[serde(default)]
    channel_id: Option<String>,
    #[serde(default)]
    stream_key: Option<String>,
}

#[derive(Deserialize)]
struct StreamEvent {
    #[serde(default)]
    stream_key: Option<String>,
    #[serde(default)]
    reason: Option<String>,
}

// =====================================================================
// Gateway client (satu akun, satu loop select, tanpa lock)
// =====================================================================
#[derive(Clone, Copy)]
enum Action {
    JoinVoice,
    StartShare { clear_rejoin: bool },
    Unpause,
}

enum Exit {
    Shutdown,
    Fatal(String),
    Reconnect(String),
}

struct Client {
    token: String,
    guild_id: Option<String>,
    channel_id: String,
    self_mute: bool,
    self_deaf: bool,
    self_video: bool,
    username: String,
    seq: Option<u64>,
    user_id: Option<String>,
    session_id: Option<String>,
    resume_url: Option<String>,
    stream_key: Option<String>,
    stream_active: bool,
    got_ready: bool,
    leaving: bool,
    voice_rtc: Option<RtcTask>,
    stream_rtc: Option<RtcTask>,
    rejoin_pending: bool,
    rejoin_count: u32,
    last_rejoin: Option<Instant>,
    actions: Vec<(Instant, Action)>,
    fatal_tx: UnboundedSender<()>,
    state: State,
    session_start: Instant,
    prior_seconds: f64,
    next_refresh: Instant,
}

impl Client {
    fn new(token: String, target: Target, state: State, fatal_tx: UnboundedSender<()>) -> Self {
        let prior_seconds = state.total_streamed_seconds as f64;
        Client {
            token,
            guild_id: target.guild_id,
            channel_id: target.channel_id,
            self_mute: state.self_mute,
            self_deaf: state.self_deaf,
            self_video: state.self_video,
            username: "Unknown".to_string(),
            seq: None,
            user_id: None,
            session_id: None,
            resume_url: None,
            stream_key: None,
            stream_active: false,
            got_ready: false,
            leaving: false,
            voice_rtc: None,
            stream_rtc: None,
            rejoin_pending: false,
            rejoin_count: 0,
            last_rejoin: None,
            actions: Vec::new(),
            fatal_tx,
            state,
            session_start: Instant::now(),
            prior_seconds,
            next_refresh: Instant::now() + REFRESH_EVERY,
        }
    }

    fn build_stream_key(&self, uid: &str) -> String {
        match &self.guild_id {
            Some(g) => format!("guild:{g}:{}:{uid}", self.channel_id),
            None => format!("call:{}:{uid}", self.channel_id),
        }
    }

    fn stop_rtc(&mut self) {
        self.voice_rtc = None;
        self.stream_rtc = None;
    }

    fn reset_session(&mut self) {
        self.session_id = None;
        self.seq = None;
        self.resume_url = None;
    }

    fn schedule(&mut self, after: Duration, a: Action) {
        self.actions.push((Instant::now() + after, a));
    }

    fn save_progress(&mut self) {
        let total = self.prior_seconds + self.session_start.elapsed().as_secs_f64();
        self.state.total_streamed_seconds = total as u64;
        save_state(&self.state);
    }

    /// Mengembalikan Some(pesan) jika berhenti karena error fatal.
    async fn run_forever(
        &mut self,
        rx: &mut UnboundedReceiver<()>,
        shutdown: &mut watch::Receiver<bool>,
    ) -> Option<String> {
        let mut backoff = GATEWAY_RETRY_MIN;
        loop {
            if *shutdown.borrow() {
                return None;
            }
            let exit = match self.drive(rx, shutdown).await {
                Ok(e) => e,
                Err(e) => Exit::Reconnect(e.to_string()),
            };
            match exit {
                Exit::Shutdown => return None,
                Exit::Fatal(msg) => {
                    eprintln!("[!] {msg}");
                    return Some(msg);
                }
                Exit::Reconnect(reason) => {
                    if self.got_ready {
                        backoff = GATEWAY_RETRY_MIN;
                        self.got_ready = false;
                    }
                    println!(
                        "[!] Connection lost: {reason}. Reconnect in {}s",
                        backoff.as_secs()
                    );
                    tokio::select! {
                        _ = sleep(backoff) => {}
                        _ = shutdown.changed() => return None,
                    }
                    backoff = (backoff * 2).min(RETRY_MAX);
                }
            }
        }
    }

    async fn drive(
        &mut self,
        rx: &mut UnboundedReceiver<()>,
        shutdown: &mut watch::Receiver<bool>,
    ) -> Res<Exit> {
        let resuming = self.session_id.is_some() && self.seq.is_some();
        let url = match (&self.resume_url, resuming) {
            (Some(u), true) => format!("{u}{GATEWAY_QUERY}"),
            _ => GATEWAY_URL.to_string(),
        };
        self.actions.clear();
        self.rejoin_pending = false;

        let ws = ws_connect(&url).await?;
        let (mut sink, mut stream) = ws.split();

        let mut inflater = Decompress::new(true);
        let mut buf: Vec<u8> = Vec::new();
        let mut hb_interval: Option<Duration> = None;
        let mut next_hb: Option<Instant> = None;
        let mut last_rx = Instant::now();

        loop {
            let next_action = self.actions.iter().map(|(t, _)| *t).min();
            tokio::select! {
                _ = shutdown.changed() => {
                    self.graceful_leave(&mut sink).await;
                    return Ok(Exit::Shutdown);
                }

                _ = sleep_until_opt(next_hb) => {
                    if last_rx.elapsed() > RECV_TIMEOUT {
                        return Err("gateway: tidak ada data (zombie)".into());
                    }
                    send_json(&mut sink, json!({"op": 1, "d": self.seq})).await?;
                    next_hb = hb_interval.map(|i| Instant::now() + i);
                }

                _ = sleep_until_opt(next_action) => {
                    self.run_due(&mut sink).await?;
                }

                _ = sleep_until(self.next_refresh) => {
                    self.next_refresh = Instant::now() + REFRESH_EVERY;
                    self.on_refresh(&mut sink).await?;
                }

                Some(()) = rx.recv() => {
                    // Sesi RTC hangus -> join ulang voice + stream.
                    self.rejoin_media();
                }

                item = stream.next() => {
                    let msg = match item {
                        Some(Ok(m)) => m,
                        Some(Err(e)) => return Err(e.into()),
                        None => return Ok(Exit::Reconnect("koneksi ditutup".to_string())),
                    };
                    last_rx = Instant::now();

                    // Lepas buffer besar (mis. dari READY) sebelum pesan berikutnya.
                    if buf.capacity() > 256 * 1024 {
                        buf = Vec::new();
                    }
                    let payload: &[u8] = match &msg {
                        Message::Binary(b) => {
                            inflate(&mut inflater, b, &mut buf)?;
                            &buf[..]
                        }
                        Message::Text(t) => t.as_bytes(),
                        Message::Close(frame) => {
                            let code = frame.as_ref().map(|f| u16::from(f.code)).unwrap_or(1000);
                            if FATAL_GATEWAY_CODES.contains(&code) {
                                return Ok(Exit::Fatal(format!(
                                    "Gateway menolak koneksi (kode {code}). Cek TOKEN. Bot berhenti."
                                )));
                            }
                            return Ok(Exit::Reconnect(format!(
                                "Server closed connection (code {code})"
                            )));
                        }
                        _ => continue,
                    };

                    let env: Envelope = match serde_json::from_slice(payload) {
                        Ok(e) => e,
                        Err(_) => continue,
                    };
                    if let Some(s) = env.s {
                        self.seq = Some(s);
                    }

                    match env.op {
                        10 => {
                            let hello: Hello = serde_json::from_str(
                                env.d.map(|r| r.get()).unwrap_or("{}"),
                            )?;
                            let interval = Duration::from_millis(hello.heartbeat_interval as u64);
                            hb_interval = Some(interval);
                            // Heartbeat pertama dengan jitter, sesuai dokumentasi Discord.
                            next_hb = Some(Instant::now() + interval.mul_f64(rand::random::<f64>()));
                            if resuming {
                                send_json(&mut sink, json!({
                                    "op": 6,
                                    "d": {
                                        "token": self.token,
                                        "session_id": self.session_id,
                                        "seq": self.seq
                                    }
                                })).await?;
                            } else {
                                self.send_identify(&mut sink).await?;
                            }
                        }
                        1 => {
                            send_json(&mut sink, json!({"op": 1, "d": self.seq})).await?;
                        }
                        7 => {
                            return Ok(Exit::Reconnect("Discord meminta reconnect (op 7)".to_string()));
                        }
                        9 => {
                            let resumable = env
                                .d
                                .and_then(|r| serde_json::from_str::<bool>(r.get()).ok())
                                .unwrap_or(false);
                            if !resumable {
                                self.reset_session();
                            }
                            return Ok(Exit::Reconnect("Invalid session (op 9)".to_string()));
                        }
                        0 => {
                            let raw = env.d.map(|r| r.get()).unwrap_or("null");
                            self.dispatch(env.t.unwrap_or(""), raw, &mut sink).await?;
                        }
                        _ => {}
                    }
                }
            }
        }
    }

    async fn send_identify(&self, sink: &mut WsSink) -> Res<()> {
        send_json(
            sink,
            json!({
                "op": 2,
                "d": {
                    "token": self.token,
                    "capabilities": 16381,
                    "properties": client_props(),
                    "presence": {"status": "online", "since": 0, "activities": [], "afk": false},
                    "compress": false,
                    "client_state": {
                        "guild_versions": {},
                        "highest_last_message_id": "0",
                        "read_state_version": 0,
                        "user_guild_settings_version": -1,
                        "user_settings_version": -1,
                        "private_channels_version": "0",
                        "api_code_version": 0
                    }
                }
            }),
        )
        .await
    }

    async fn dispatch(&mut self, event: &str, raw: &str, sink: &mut WsSink) -> Res<()> {
        match event {
            "READY" => {
                let r: Ready = serde_json::from_str(raw)?;
                self.username = r.user.username.unwrap_or_else(|| "User".to_string());
                println!("[+] Logged in: {} (ID: {})", self.username, r.user.id);
                self.user_id = Some(r.user.id);
                self.session_id = Some(r.session_id);
                self.resume_url = r.resume_gateway_url;
                self.got_ready = true;
                self.stream_active = false;
                self.stop_rtc();
                self.join_voice(sink).await?;
                self.schedule(
                    Duration::from_millis(1200),
                    Action::StartShare {
                        clear_rejoin: false,
                    },
                );
            }

            "RESUMED" => {
                self.got_ready = true;
                println!("[+] Session resumed");
            }

            "VOICE_STATE_UPDATE" => {
                if let Ok(v) = serde_json::from_str::<VoiceStateUpdate>(raw) {
                    if Some(&v.user_id) == self.user_id.as_ref() {
                        if let Some(sid) = v.session_id {
                            self.session_id = Some(sid);
                        }
                        if v.channel_id.is_none() {
                            self.rejoin_media();
                        }
                    }
                }
            }

            "VOICE_SERVER_UPDATE" => {
                if let Ok(v) = serde_json::from_str::<ServerUpdate>(raw) {
                    if let (Some(endpoint), Some(token), Some(user_id), Some(session_id)) = (
                        v.endpoint,
                        v.token,
                        self.user_id.clone(),
                        self.session_id.clone(),
                    ) {
                        let server_id = v
                            .guild_id
                            .or(v.channel_id)
                            .unwrap_or_else(|| self.channel_id.clone());
                        self.voice_rtc = Some(spawn_rtc(
                            RtcParams {
                                endpoint,
                                server_id,
                                channel_id: self.channel_id.clone(),
                                user_id,
                                session_id,
                                token,
                                is_stream: false,
                            },
                            self.fatal_tx.clone(),
                        ));
                    }
                }
            }

            "STREAM_CREATE" => {
                if let Ok(v) = serde_json::from_str::<StreamEvent>(raw) {
                    if let Some(key) = v.stream_key {
                        println!("[+] Screen Share active! Stream Key: {key}");
                        self.stream_key = Some(key);
                        self.stream_active = true;
                        self.set_paused(sink, false).await?;
                    }
                }
            }

            "STREAM_SERVER_UPDATE" => {
                if let Ok(v) = serde_json::from_str::<ServerUpdate>(raw) {
                    let has_key = v.stream_key.is_some() || self.stream_key.is_some();
                    if let (Some(endpoint), Some(token), Some(user_id), Some(session_id), true) = (
                        v.endpoint,
                        v.token,
                        self.user_id.clone(),
                        self.session_id.clone(),
                        has_key,
                    ) {
                        let server_id = v
                            .guild_id
                            .or_else(|| self.guild_id.clone())
                            .unwrap_or_else(|| self.channel_id.clone());
                        self.stream_rtc = Some(spawn_rtc(
                            RtcParams {
                                endpoint,
                                server_id,
                                channel_id: self.channel_id.clone(),
                                user_id,
                                session_id,
                                token,
                                is_stream: true,
                            },
                            self.fatal_tx.clone(),
                        ));
                    }
                }
            }

            "STREAM_DELETE" => {
                self.stream_active = false;
                if !self.leaving {
                    let reason = serde_json::from_str::<StreamEvent>(raw)
                        .ok()
                        .and_then(|v| v.reason)
                        .unwrap_or_else(|| "unknown".to_string());
                    println!("[!] Stream ended (reason: {reason}). Restarting...");
                    self.schedule(
                        Duration::from_millis(2500),
                        Action::StartShare {
                            clear_rejoin: false,
                        },
                    );
                }
            }

            _ => {}
        }
        Ok(())
    }

    /// Jadwalkan join voice + mulai screen share lagi. Jeda naik bertahap
    /// (3s, 6s, ... maks 2 menit); kembali ke 3s jika >10 menit sejak terakhir.
    fn rejoin_media(&mut self) {
        if self.rejoin_pending || self.leaving {
            return;
        }
        self.rejoin_pending = true;
        let now = Instant::now();
        let stale = self
            .last_rejoin
            .map_or(true, |t| now.duration_since(t) > Duration::from_secs(600));
        if stale {
            self.rejoin_count = 0;
        }
        let delay = rejoin_delay(self.rejoin_count, RETRY_MAX);
        self.rejoin_count += 1;
        self.last_rejoin = Some(now);
        println!(
            "[!] Voice/stream terputus, rejoin dalam {}s...",
            delay.as_secs()
        );
        self.schedule(delay, Action::JoinVoice);
        self.schedule(
            delay + Duration::from_secs(1),
            Action::StartShare { clear_rejoin: true },
        );
    }

    async fn run_due(&mut self, sink: &mut WsSink) -> Res<()> {
        let now = Instant::now();
        let (mut due, rest): (Vec<_>, Vec<_>) = std::mem::take(&mut self.actions)
            .into_iter()
            .partition(|(t, _)| *t <= now);
        self.actions = rest;
        due.sort_by_key(|(t, _)| *t);
        for (_, action) in due {
            match action {
                Action::JoinVoice => self.join_voice(sink).await?,
                Action::StartShare { clear_rejoin } => {
                    if clear_rejoin {
                        self.rejoin_pending = false;
                    }
                    self.start_share(sink).await?;
                }
                Action::Unpause => self.set_paused(sink, false).await?,
            }
        }
        Ok(())
    }

    async fn join_voice(&self, sink: &mut WsSink) -> Res<()> {
        send_json(
            sink,
            json!({
                "op": 4,
                "d": {
                    "guild_id": self.guild_id,
                    "channel_id": self.channel_id,
                    "self_mute": self.self_mute,
                    "self_deaf": self.self_deaf,
                    "self_video": self.self_video
                }
            }),
        )
        .await?;
        println!("[*] Joining voice channel ({})...", self.channel_id);
        Ok(())
    }

    async fn start_share(&mut self, sink: &mut WsSink) -> Res<()> {
        let Some(uid) = self.user_id.clone() else {
            return Ok(());
        };
        let kind = if self.guild_id.is_some() {
            "guild"
        } else {
            "call"
        };
        send_json(
            sink,
            json!({
                "op": 18,
                "d": {
                    "type": kind,
                    "guild_id": self.guild_id,
                    "channel_id": self.channel_id,
                    "preferred_region": null
                }
            }),
        )
        .await?;
        self.stream_key = Some(self.build_stream_key(&uid));
        self.schedule(Duration::from_millis(500), Action::Unpause);
        Ok(())
    }

    async fn set_paused(&self, sink: &mut WsSink, paused: bool) -> Res<()> {
        let key = match (&self.stream_key, &self.user_id) {
            (Some(k), _) => k.clone(),
            (None, Some(u)) => self.build_stream_key(u),
            _ => return Ok(()),
        };
        send_json(
            sink,
            json!({"op": 22, "d": {"stream_key": key, "paused": paused}}),
        )
        .await
    }

    async fn on_refresh(&mut self, sink: &mut WsSink) -> Res<()> {
        if self.stream_active {
            self.set_paused(sink, false).await?;
        }
        let elapsed = self.session_start.elapsed().as_secs_f64();
        let total_hours = (self.prior_seconds + elapsed) / 3600.0;
        self.save_progress();
        println!(
            "[{}] Session: {} | Total: {:.2}h | Next: {}",
            if self.stream_active {
                "LIVE"
            } else {
                "CONNECTING"
            },
            format_duration(elapsed as u64),
            total_hours,
            next_badge(total_hours)
        );
        Ok(())
    }

    async fn graceful_leave(&mut self, sink: &mut WsSink) {
        self.leaving = true;
        if let Some(key) = &self.stream_key {
            let _ = send_json(sink, json!({"op": 19, "d": {"stream_key": key}})).await;
        }
        let _ = send_json(
            sink,
            json!({
                "op": 4,
                "d": {
                    "guild_id": self.guild_id,
                    "channel_id": null,
                    "self_mute": true,
                    "self_deaf": true,
                    "self_video": false
                }
            }),
        )
        .await;
        let _ = timeout(Duration::from_secs(3), sink.close()).await;
        self.stop_rtc();
    }
}

// =====================================================================
// Main
// =====================================================================
#[tokio::main(flavor = "current_thread")]
async fn main() {
    // Provider kripto rustls harus dipasang eksplisit (versi 0.23).
    let _ = rustls::crypto::ring::default_provider().install_default();

    println!("[*] Stream keep-alive (Rust lite) | {SERVER_INVITE}");

    let state = load_state();
    let token = env::var("TOKEN").unwrap_or_default().trim().to_string();
    let room_input = env::var("ROOM_INPUT")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(|| state.room_input.clone())
        .trim()
        .to_string();

    // Config salah -> diam saja (bukan crash-loop yang terus login ke Discord).
    if token.is_empty() {
        eprintln!("[!] Error: environment variable TOKEN kosong.");
        wait_shutdown_signal().await;
        return;
    }
    if room_input.is_empty() {
        eprintln!("[!] Error: room_input kosong (ROOM_INPUT / stream_state.json).");
        wait_shutdown_signal().await;
        return;
    }

    println!("[*] Validating token and resolving channel...");
    let resolved = tokio::task::spawn_blocking({
        let (room, tok) = (room_input.clone(), token.clone());
        move || resolve_room(&room, &tok)
    })
    .await;
    let target = match resolved {
        Ok(Ok(t)) => t,
        Ok(Err(e)) => {
            eprintln!("[!] Error: {e}");
            wait_shutdown_signal().await;
            return;
        }
        Err(e) => {
            eprintln!("[!] Error: {e}");
            wait_shutdown_signal().await;
            return;
        }
    };
    println!(
        "[+] Target: {} | Channel: {} | Guild: {}",
        target.name,
        target.channel_id,
        target.guild_id.as_deref().unwrap_or("Private/DM")
    );

    let (fatal_tx, mut fatal_rx) = unbounded_channel::<()>();
    let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
    let mut client = Client::new(token, target, state, fatal_tx);

    let fatal = {
        let run = client.run_forever(&mut fatal_rx, &mut shutdown_rx);
        tokio::pin!(run);
        tokio::select! {
            r = &mut run => r,
            _ = wait_shutdown_signal() => {
                println!("[*] Stopping and leaving voice channel...");
                let _ = shutdown_tx.send(true);
                timeout(Duration::from_secs(8), &mut run).await.ok().flatten()
            }
        }
    };

    client.save_progress();
    if fatal.is_some() {
        // Token ditolak: jangan retry/restart terus-menerus, tunggu dihentikan.
        wait_shutdown_signal().await;
    }
    println!("[+] Disconnected cleanly.");
}
