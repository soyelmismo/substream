package spotify

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const spotifyWorkerScript = `
import sys, os, time, base64, json, subprocess, requests

try:
    from pywidevine.cdm import Cdm
    from pywidevine.device import Device
    from pywidevine.pssh import PSSH
except ImportError as e:
    print(f"ERR pywidevine not installed: {e}", flush=True)
    sys.exit(1)

wvd_path = sys.argv[1] if len(sys.argv) > 1 else "spotify.wvd"
sp_dc = sys.argv[2] if len(sys.argv) > 2 else ""
cache_dir = sys.argv[3] if len(sys.argv) > 3 else "cache/spotify"

os.makedirs(cache_dir, exist_ok=True)

if not os.path.isfile(wvd_path):
    print(f"ERR wvd file not found at: {wvd_path}", flush=True)
    sys.exit(1)

try:
    device = Device.load(wvd_path)
    cdm = Cdm.from_device(device)
except Exception as e:
    print(f"ERR failed to load wvd device: {e}", flush=True)
    sys.exit(1)

BASE62 = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
def track_id_to_gid(track_id: str) -> str:
    n = 0
    for char in track_id:
        n = n * 62 + BASE62.index(char)
    return f"{n:032x}"

WIDEVINE_SYSTEM_ID = b'\xed\xef\x8b\xa9\x79\xd6\x4a\xce\xa3\xc8\x27\xdc\xd5\x1d\x21\xed'
def extract_widevine_pssh(data: bytes) -> str:
    offset = 0
    while True:
        pos = data.find(b'pssh', offset)
        if pos == -1:
            break
        size = int.from_bytes(data[pos-4:pos], byteorder='big')
        box = data[pos-4:pos-4+size]
        if len(box) >= 28 and box[12:28] == WIDEVINE_SYSTEM_ID:
            return base64.b64encode(box).decode('ascii')
        offset = pos + 4
    return None

cached_token = ""
token_expires_at = 0

def get_token(fallback_token=""):
    global cached_token, token_expires_at
    if fallback_token:
        return fallback_token
    now = time.time()
    if cached_token and now < (token_expires_at - 60):
        return cached_token
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    }
    if sp_dc:
        headers["Cookie"] = f"sp_dc={sp_dc}"
    resp = requests.get("https://open.spotify.com/get_access_token?reason=transport&productType=web_player", headers=headers, timeout=10)
    if resp.status_code == 200:
        data = resp.json()
        cached_token = data.get("accessToken", "")
        exp_ms = data.get("accessTokenExpirationTimestampMs", 0)
        token_expires_at = exp_ms / 1000.0 if exp_ms else now + 3600
        return cached_token
    return fallback_token

print("READY", flush=True)

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    parts = line.split()
    track_id = parts[0]
    token_param = parts[1] if len(parts) > 1 else ""

    out_file = os.path.abspath(os.path.join(cache_dir, f"{track_id}.m4a"))
    if os.path.exists(out_file) and os.path.getsize(out_file) > 10000:
        print(f"OK {out_file}", flush=True)
        continue

    try:
        token = get_token(token_param)
        if not token:
            raise Exception("no spotify access token available (check sp_dc)")

        gid = track_id_to_gid(track_id)
        meta_headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
            "App-Platform": "WebPlayer",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        }

        # 1. Fetch metadata to get file_id for MP4_128 (Free) or MP4_256
        r = requests.get(f"https://spclient.wg.spotify.com/metadata/4/track/{gid}", headers=meta_headers, timeout=10)
        if r.status_code != 200:
            raise Exception(f"metadata returned HTTP {r.status_code}")
        meta = r.json()

        file_id = None
        # Try MP4_128 first for Free tier
        for f in meta.get("file", []):
            if f.get("format") == "MP4_128":
                file_id = f.get("file_id")
                break
        if not file_id:
            for f in meta.get("file", []):
                if "MP4" in f.get("format", ""):
                    file_id = f.get("file_id")
                    break
        if not file_id:
            raise Exception("no MP4 file format found in track metadata")

        # 2. Resolve CDN URL
        r = requests.get(f"https://gew4-spclient.spotify.com/storage-resolve/v2/files/audio/interactive/11/{file_id}?alt=json", headers=meta_headers, timeout=10)
        if r.status_code != 200:
            raise Exception(f"storage-resolve returned HTTP {r.status_code}")
        cdn_urls = r.json().get("cdnurl", [])
        if not cdn_urls:
            raise Exception("storage-resolve returned empty cdnurl")
        cdn_url = cdn_urls[0]

        # 3. Read initial 32KB to extract PSSH box
        head_r = requests.get(cdn_url, headers={"Range": "bytes=0-32767", "User-Agent": meta_headers["User-Agent"]}, timeout=10)
        pssh_b64 = extract_widevine_pssh(head_r.content)
        if not pssh_b64:
            raise Exception("could not find Widevine PSSH box in media header")

        # 4. Request Widevine license
        session_id = cdm.open()
        challenge = cdm.get_license_challenge(session_id, PSSH(pssh_b64))
        lic_resp = requests.post(
            "https://gew4-spclient.spotify.com/widevine-license/v1/audio/license",
            data=challenge,
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/octet-stream",
                "User-Agent": meta_headers["User-Agent"]
            },
            timeout=10
        )
        if lic_resp.status_code != 200:
            cdm.close(session_id)
            raise Exception(f"license request rejected: HTTP {lic_resp.status_code}")

        cdm.parse_license(session_id, lic_resp.content)
        key_hex = None
        for k in cdm.get_keys(session_id):
            if k.type == 'CONTENT':
                key_hex = k.key.hex()
                break
        cdm.close(session_id)

        if not key_hex:
            raise Exception("no content decryption key found in license response")

        # 5. Decrypt via ffmpeg stream-copy into local cache
        temp_file = out_file + ".tmp"
        cmd = [
            "ffmpeg", "-y", "-loglevel", "error",
            "-cenc_decryption_key", key_hex,
            "-i", cdn_url,
            "-c", "copy",
            temp_file
        ]
        res = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if res.returncode != 0:
            raise Exception(f"ffmpeg decryption error: {res.stderr.strip()}")

        os.replace(temp_file, out_file)
        print(f"OK {out_file}", flush=True)

    except Exception as ex:
        print(f"ERR {ex}", flush=True)
`

// WidevineWorker manages the persistent Python process for Spotify Widevine DRM decryption
type WidevineWorker struct {
	mu         sync.Mutex
	wvdPath    string
	spDC       string
	cacheDir   string
	cmd        *exec.Cmd
	stdin      io.WriteCloser
	reader     *bufio.Reader
	available  bool
	inFlightMu sync.Mutex
	inFlight   map[string]*inFlightJob
}

type inFlightJob struct {
	done chan struct{}
	path string
	err  error
}

// NewWidevineWorker creates a worker handle
func NewWidevineWorker(wvdPath, spDC, cacheDir string) *WidevineWorker {
	if cacheDir == "" {
		cacheDir = "cache/spotify"
	}
	_ = os.MkdirAll(cacheDir, 0755)

	w := &WidevineWorker{
		wvdPath:  wvdPath,
		spDC:     spDC,
		cacheDir: cacheDir,
		inFlight: make(map[string]*inFlightJob),
	}
	return w
}

// IsConfigured returns true if WVD file is present
func (w *WidevineWorker) IsConfigured() bool {
	if w.wvdPath == "" {
		return false
	}
	_, err := os.Stat(w.wvdPath)
	return err == nil
}

// Warmup initializes the resident python process in the background
func (w *WidevineWorker) Warmup() {
	if !w.IsConfigured() {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.ensureWorkerLocked(); err != nil {
		log.Printf("[SPOTIFY-WVD] Note: Worker warmup failed (%v), will retry on demand", err)
	}
}

func (w *WidevineWorker) ensureWorkerLocked() error {
	if w.cmd != nil && w.cmd.Process != nil && w.stdin != nil && w.available {
		return nil
	}

	absWVD, err := filepath.Abs(w.wvdPath)
	if err != nil {
		absWVD = w.wvdPath
	}
	absCache, err := filepath.Abs(w.cacheDir)
	if err != nil {
		absCache = w.cacheDir
	}

	cmd := exec.Command("python3", "-u", "-c", spotifyWorkerScript, absWVD, w.spDC, absCache)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return fmt.Errorf("worker stdin pipe: %w", err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		_ = stdin.Close()
		return fmt.Errorf("worker stdout pipe: %w", err)
	}

	if err := cmd.Start(); err != nil {
		_ = stdin.Close()
		return fmt.Errorf("start worker process: %w", err)
	}

	reader := bufio.NewReader(stdout)
	readyChan := make(chan string, 1)
	errChan := make(chan error, 1)

	go func() {
		line, err := reader.ReadString('\n')
		if err != nil {
			errChan <- err
			return
		}
		readyChan <- line
	}()

	select {
	case line := <-readyChan:
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "READY") {
			_ = cmd.Process.Kill()
			return fmt.Errorf("worker init greeting error: %s", line)
		}
	case err := <-errChan:
		_ = cmd.Process.Kill()
		return fmt.Errorf("worker read init error: %w", err)
	case <-time.After(8 * time.Second):
		_ = cmd.Process.Kill()
		return fmt.Errorf("worker startup timeout")
	}

	w.cmd = cmd
	w.stdin = stdin
	w.reader = reader
	w.available = true
	log.Printf("[SPOTIFY-WVD] 🚀 Persistent Widevine CDM worker initialized (wvd: %s, cache: %s)", w.wvdPath, w.cacheDir)
	return nil
}

// Resolve decrypts and downloads a track via Widevine CDM into local cache, returning the file path
func (w *WidevineWorker) Resolve(ctx context.Context, trackID, token string) (string, error) {
	trackID = strings.TrimSpace(trackID)
	if trackID == "" {
		return "", fmt.Errorf("empty trackID")
	}

	// 1. Fast cache check
	cacheFile := filepath.Join(w.cacheDir, fmt.Sprintf("%s.m4a", trackID))
	if info, err := os.Stat(cacheFile); err == nil && info.Size() > 10000 {
		return cacheFile, nil
	}

	// 2. Singleflight: deduplicate simultaneous calls for the same track
	w.inFlightMu.Lock()
	if job, running := w.inFlight[trackID]; running {
		w.inFlightMu.Unlock()
		select {
		case <-job.done:
			return job.path, job.err
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}

	job := &inFlightJob{done: make(chan struct{})}
	w.inFlight[trackID] = job
	w.inFlightMu.Unlock()

	var resPath string
	var resErr error

	defer func() {
		job.path = resPath
		job.err = resErr
		close(job.done)

		w.inFlightMu.Lock()
		delete(w.inFlight, trackID)
		w.inFlightMu.Unlock()
	}()

	// 3. Worker extraction
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.ensureWorkerLocked(); err != nil {
		resErr = fmt.Errorf("ensure worker: %w", err)
		return "", resErr
	}

	input := fmt.Sprintf("%s %s\n", trackID, token)
	if _, err := io.WriteString(w.stdin, input); err != nil {
		w.available = false
		if w.cmd != nil && w.cmd.Process != nil {
			_ = w.cmd.Process.Kill()
		}
		resErr = fmt.Errorf("write to worker stdin: %w", err)
		return "", resErr
	}

	type workerResult struct {
		line string
		err  error
	}
	ch := make(chan workerResult, 1)
	go func() {
		line, err := w.reader.ReadString('\n')
		ch <- workerResult{line: strings.TrimSpace(line), err: err}
	}()

	select {
	case res := <-ch:
		if res.err != nil {
			w.available = false
			if w.cmd != nil && w.cmd.Process != nil {
				_ = w.cmd.Process.Kill()
			}
			resErr = fmt.Errorf("worker communication error: %w", res.err)
			return "", resErr
		}
		if strings.HasPrefix(res.line, "OK ") {
			resPath = strings.TrimSpace(strings.TrimPrefix(res.line, "OK "))
			return resPath, nil
		}
		errMsg := strings.TrimPrefix(res.line, "ERR ")
		resErr = fmt.Errorf("worker extraction failed: %s", errMsg)
		return "", resErr
	case <-ctx.Done():
		resErr = ctx.Err()
		return "", resErr
	case <-time.After(35 * time.Second):
		w.available = false
		if w.cmd != nil && w.cmd.Process != nil {
			_ = w.cmd.Process.Kill()
		}
		resErr = fmt.Errorf("worker execution timed out (35s)")
		return "", resErr
	}
}
