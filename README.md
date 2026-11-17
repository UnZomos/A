# stream-keepalive

Program Rust ringan yang menjaga sesi streaming tetap hidup (*keepalive*) dengan pemakaian RAM dan jaringan seminimal mungkin. Dirancang untuk berjalan terus-menerus di server kecil atau platform cloud seperti Railway.

> **Branch:** `rust`

## Fitur

- Koneksi WebSocket persisten dengan TLS (`tokio-tungstenite` + `rustls`)
- Runtime async yang hemat resource (`tokio`)
- Menyimpan status sesi ke `stream_state.json`, jadi total durasi streaming tetap terlacak setelah restart
- Shutdown rapi lewat sinyal (Ctrl+C / SIGTERM)
- Binary dioptimalkan untuk ukuran kecil (`opt-level = "s"`, LTO, `strip`)
- Siap deploy dengan Docker

## Struktur proyek

```
.
├── src/                 # Kode sumber Rust
├── Cargo.toml           # Dependensi dan profil build
├── Dockerfile           # Build image untuk deploy
└── stream_state.json    # Status sesi (dibuat/diperbarui otomatis)
```

## Persyaratan

- Rust (stable terbaru, edisi 2021) dan Cargo
- atau Docker, kalau mau menjalankan lewat container

## Menjalankan

### Lokal

```bash
git clone -b rust https://github.com/UnZomos/A.git
cd A
cargo run --release
```

Binary hasil build ada di `target/release/stream-keepalive`.

### Docker

```bash
docker build -t stream-keepalive .
docker run --rm stream-keepalive
```

### Railway

Dockerfile memakai image `rust:1-slim` karena Rust bawaan Railway (1.84) terlalu lama untuk beberapa crate. Cukup hubungkan repo (branch `rust`) dan Railway akan build otomatis dari `Dockerfile`.

## Konfigurasi

<!-- TODO: isi dengan environment variable yang dipakai di src/ -->

| Variabel | Wajib | Keterangan |
| -------- | ----- | ---------- |
| _(isi)_  | _(ya/tidak)_ | _(isi)_ |

> Jangan commit token, kredensial, atau rahasia lain ke repositori. Simpan di environment variable atau secret manager platform deploy.

### `stream_state.json`

File status sesi dengan format:

```json
{
  "room_input": "<id-room>",
  "self_mute": true,
  "self_deaf": true,
  "self_video": false,
  "total_streamed_seconds": 0
}
```

| Field | Arti |
| ----- | ---- |
| `room_input` | ID room tujuan |
| `self_mute` | Mikrofon dimatikan |
| `self_deaf` | Audio masuk dimatikan |
| `self_video` | Video dinyalakan atau tidak |
| `total_streamed_seconds` | Akumulasi durasi streaming (detik) |

## Dependensi utama

| Crate | Fungsi |
| ----- | ------ |
| `tokio` | Runtime async, timer, jaringan, sinyal |
| `tokio-tungstenite` | Klien WebSocket (TLS via rustls) |
| `rustls` | TLS tanpa OpenSSL |
| `ureq` | Klien HTTP sederhana |
| `flate2` | Kompresi/dekompresi |
| `serde`, `serde_json` | Serialisasi status dan pesan JSON |
| `base64`, `rand`, `uuid` | Utilitas encoding dan ID acak |

## Rencana pengembangan

- [ ] Menambah frame/gambar ringan agar sesi tampak seperti streaming sungguhan, dengan batas RAM dan jaringan yang rendah
- [ ] Merapikan struktur file di repositori
- [ ] Melengkapi dokumentasi konfigurasi

## Lisensi

<!-- TODO: tambahkan lisensi (mis. MIT) -->
