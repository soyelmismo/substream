<p align="center">
  <b>SubStream</b>
</p>

<p align="center">
  <b>Infinite OpenSubsonic server. Zero local files. Powered by Hi-Fi Cloud APIs.</b>
</p>

<div align="center">
  <a href="https://github.com/soyelmismo/substream/releases">
      <img alt="Releases" src="https://img.shields.io/github/v/release/soyelmismo/substream.svg?style=flat">
  </a>
  <a href="https://www.gnu.org/licenses/gpl-3.0">
      <img src="https://img.shields.io/badge/license-GPL%20v3-blue.svg?style=flat">
  </a>
</div>

---

**SubStream** is a paradigm shift in self-hosted music. Instead of managing terabytes of local audio files, SubStream acts as a lightweight, lightning-fast proxy that exposes an infinite cloud music catalog through the standard OpenSubsonic API. 

Use your favorite Subsonic clients (such as Tempus, Symfonium, DSub, or Supersonic) to seamlessly browse, search, and stream from a limitless cloud library.

## Architecture & Concept

SubStream is built on the foundations of [Gonic](https://github.com/sentriz/gonic), but stripped of everything related to local file management:
- **No Scanner:** No files to read. No ID3 parsing. 
- **No Transcoding:** Audio streams and Cover Arts are proxied seamlessly from upstream CDNs without server-side transcoding, ensuring lossless quality while sidestepping CORS and certificate issues for clients.
- **Stateless Metadata:** Track metadata, artists, and albums are fetched in real-time from upstream providers and translated to OpenSubsonic XML/JSON structures on the fly.
- **Local SQLite DB:** Strictly minimized to store configurations, user credentials, custom playlists, favorites (stars), play queue state, and bookmarks (IDs only).

## Features

- **Infinite Library:** Play any track, album, or artist available on the configured upstream catalogs.
- **Zero Local Audio Storage:** Streaming data is buffered directly from the provider's CDN through SubStream to your client. No media files are ever stored on your hard drive.
- **Dynamic Proxy Pool:** Built-in high-availability worker pool for external Hi-Fi API requests, featuring auto-discovery, health checks, and automatic failover to prevent upstream limits.
- **High Performance:** Concurrent batch-fetching, 3-second circuit breakers for local database lookups, and a robust global semaphore ensure your database and network requests remain responsive.
- **Subsonic API Compliance:** Compatible with all well-behaved OpenSubsonic parameters, mapping IDs (`tr-`, `ar-`, `al-`) transparently to native backend IDs. 
- **Scrobbling:** Built-in native support for sending now-playing and scrobbles to **Last.fm** and **ListenBrainz**.
- **Admin Panel:** Sleek web interface to manage users, configure Last.fm integration, and manage the health of your API proxy instances.

## Legal Disclaimer

> **Disclaimer:** SubStream is solely an API translation proxy. It **does not host, store, or distribute** any copyrighted audio files, metadata, or images. SubStream interacts dynamically with user-provided, community-operated external API instances. The user assumes all responsibility for adhering to the Terms of Service of any third-party streaming service accessed through these external instances.

## Installation

the default login is **admin**/**admin**.  
password can then be changed from the web interface.

### Compile from source
```bash
git clone https://github.com/soyelmismo/substream.git
cd substream
go build -o substream cmd/substream/main.go
./substream
```

## Running SubStream

Launch the executable. SubStream requires no complex configuration for path mapping. 

| Env Var | CLI Arg | Description |
|---|---|---|
| `SUBSTREAM_LISTEN_ADDR` | `-listen-addr` | Host and port to listen on (default `0.0.0.0:4533`) |
| `SUBSTREAM_DB_PATH` | `-db-path` | Path to SQLite database file (default `substream.db`) |
| `SUBSTREAM_CACHE_PATH` | `-cache-path` | Path to ephemeral cache directory |
| `SUBSTREAM_PROXY_URLS` | `-proxy-urls` | Comma-separated list of backend hifi-api proxies |

## Recommendations

For the best mobile experience, we recommend using **[Tempus](https://github.com/eddyizm/tempus)**. Tempus is uniquely built to leverage OpenSubsonic's dynamic endpoints and integrates beautifully with SubStream's cloud-backed capabilities.

## Credits

- Forked and dramatically reimagined from [Gonic](https://github.com/sentriz/gonic).
- Subsonic API specification mapping guided by the [OpenSubsonic API](https://opensubsonic.netlify.app/).
