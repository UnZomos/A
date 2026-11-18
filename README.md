# Stream Keepalive

A lightweight Rust application designed to maintain an active streaming session 24/7 with minimal CPU, memory, and bandwidth consumption. Built with asynchronous networking to run continuously on low-resource VPS instances or container platforms like Railway.

---

## Key Features

- **Asynchronous Architecture**: Powered by `tokio` for low memory usage and non-blocking I/O.
- **WebSocket & RTC Keep-Alive**: Connects directly to gateway and voice RTC sessions with compressed payloads (`zlib-stream`).
- **Minimal Video RTP Simulation**: Generates lightweight dummy H.264 frames at 1 FPS over encrypted UDP (supporting `aead_aes256_gcm_rtpsize` and `aead_xchacha20_poly1305_rtpsize`) to keep the stream state alive without encoding overhead.
- **State Persistence**: Records accumulated stream duration to `stream_state.json` so milestones remain intact across restarts.
- **Milestone Tracking**: Monitors progress against streaming badge tiers in real time.
- **Graceful Shutdown**: Intercepts `SIGINT` (Ctrl+C) and `SIGTERM` signals to leave voice channels cleanly before exiting.

---

## Project Structure

```text
.
├── src/
│   ├── main.rs          # Core application logic, gateway, and RTC handlers
│   └── util.rs          # Channel parser, badge tiers, and helper utilities
├── Cargo.toml           # Package configuration and dependencies
├── Dockerfile           # Minimal container build specification
├── LICENSE              # Open-source license
└── stream_state.json    # Local session state and accumulated duration
```

---

## Configuration

The application is configured through environment variables and a local JSON state file.

### Environment Variables

| Variable | Required | Description |
|---|---|---|
| `TOKEN` | **Yes** | User authentication token. Never commit this to source control. |
| `ROOM_INPUT` | Optional | Target channel ID, URL, or pair. Falls back to `stream_state.json` if omitted. |

### `stream_state.json`

This file is created and updated automatically. You can also preconfigure initial values:

```json
{
  "room_input": "1563278810256769116",
  "self_mute": true,
  "self_deaf": true,
  "self_video": false,
  "total_streamed_seconds": 0
}
```

- `room_input`: Channel identifier or Discord link (`https://discord.com/channels/...`).
- `self_mute` / `self_deaf`: Mute and deafen flags when joining voice.
- `self_video`: Video feed state indicator.
- `total_streamed_seconds`: Stored stream duration counter in seconds.

---

## Getting Started

### Prerequisites

- Rust toolchain (stable edition 2021) and Cargo, or Docker.

### Running Locally

```bash
# Clone the repository
git clone -b rust https://github.com/UnZomos/A.git
cd A

# Set configuration and run
export TOKEN="your_token_here"
export ROOM_INPUT="your_channel_id_or_url"
cargo run --release
```

The optimized binary will be compiled to `target/release/stream-keepalive`.

### Running with Docker

```bash
docker build -t stream-keepalive .
docker run -d \
  -e TOKEN="your_token_here" \
  -e ROOM_INPUT="your_channel_id_or_url" \
  --name stream-keepalive \
  stream-keepalive
```

---

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
