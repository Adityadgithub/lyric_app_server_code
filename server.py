
"""
FastAPI server that:
- downloads audio files from YouTube/other video links using yt-dlp (queued + cached)
- optionally fetches synced lyrics from YouTube Music (ytmusicapi)
"""
import asyncio
import hashlib
import json
import logging
import os
import re
import shutil
import sys
import tempfile
import threading
import time
from collections import defaultdict, deque
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from urllib.parse import quote, unquote, urlsplit
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

import subprocess

from fastapi import FastAPI, Query, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
import yt_dlp
from yt_dlp.utils import DownloadError
from ytmusicapi import YTMusic

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

app = FastAPI(title="Lyrical Insta API", version="1.1.0")

# Allow Flutter app to call from localhost / emulator
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def log_request_url(request: Request, call_next):
    logger.info("[API] %s %s", request.method, request.url)
    return await call_next(request)


@app.on_event("startup")
async def _startup_download_gc():
    async def _loop():
        while True:
            try:
                _purge_expired_downloads()
            except Exception as e:
                logger.warning("[downloads] GC error: %s", e)
            await asyncio.sleep(60)

    asyncio.create_task(_loop())


# Global YTMusic client (unauthenticated; uses public endpoints)
ytmusic_client = YTMusic()

_BACKEND_DIR = Path(__file__).resolve().parent
_DEFAULT_COOKIES_FILE = _BACKEND_DIR / "cookies.txt"
_COOKIE_REFRESH_INTERVAL_SECONDS = float(os.environ.get("YOUTUBE_COOKIES_REFRESH_SECONDS", "30"))
_COOKIE_TMP_PATH = os.environ.get(
    "YOUTUBE_COOKIES_TMP_PATH",
    os.path.join(tempfile.gettempdir(), "lyrical_insta_cookies.txt"),
)
_cookie_cache_lock = threading.Lock()
_cookie_cache_source: str | None = None
_cookie_cache_label: str | None = None
_cookie_cache_sig: tuple[int, int] | None = None
_cookie_cache_last_check_ts = 0.0
_cookie_missing_logged_at = 0.0

_DOWNLOAD_DIR = Path(
    os.environ.get("AUDIO_DOWNLOAD_DIR", str(_BACKEND_DIR / "downloads"))
).resolve()
_DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
_EXTERNAL_FILE_BASE_URL = os.environ.get("AUDIO_EXTERNAL_FILE_BASE_URL", "").strip().rstrip("/")

_MAX_CONCURRENT_DOWNLOADS = int(os.environ.get("AUDIO_MAX_CONCURRENT_DOWNLOADS", "3"))
_DOWNLOAD_TTL_SECONDS = int(os.environ.get("AUDIO_DOWNLOAD_TTL_SECONDS", str(60 * 60)))
_RATE_LIMIT_PER_HOUR = int(os.environ.get("AUDIO_RATE_LIMIT_PER_HOUR", "5"))

_NEWPIPE_EXTRACTOR_URL = os.environ.get("NEWPIPE_EXTRACTOR_URL", "").strip().rstrip("/")
_NEWPIPE_EXTRACTOR_TIMEOUT = float(os.environ.get("NEWPIPE_EXTRACTOR_TIMEOUT_SECONDS", "25"))

_download_executor = ThreadPoolExecutor(
    max_workers=max(1, _MAX_CONCURRENT_DOWNLOADS),
    thread_name_prefix="audio_dl",
)
_download_semaphore = asyncio.Semaphore(max(1, _MAX_CONCURRENT_DOWNLOADS))
_download_locks: dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
_rate_buckets: dict[str, deque[float]] = defaultdict(deque)


def _client_ip(request: Request) -> str:
    forwarded = request.headers.get("x-forwarded-for")
    if forwarded:
        return forwarded.split(",")[0].strip()
    if request.client:
        return request.client.host
    return "unknown"


def _build_file_url(request: Request, file_id: str) -> str:
    """
    Returns the playback URL sent back to the app.
    - If AUDIO_EXTERNAL_FILE_BASE_URL is configured, return an external/CDN URL.
    - Otherwise, fall back to local FastAPI `/get_file/{file_id}`.
    """
    encoded_file_id = quote(file_id, safe="")
    if _EXTERNAL_FILE_BASE_URL:
        return f"{_EXTERNAL_FILE_BASE_URL}/{encoded_file_id}"

    base = str(request.base_url).rstrip("/")
    return f"{base}/get_file/{encoded_file_id}"


def _enforce_rate_limit(client_ip: str) -> None:
    now = time.time()
    window_start = now - 3600.0
    bucket = _rate_buckets[client_ip]
    while bucket and bucket[0] < window_start:
        bucket.popleft()
    if len(bucket) >= _RATE_LIMIT_PER_HOUR:
        raise HTTPException(
            status_code=429,
            detail=f"Rate limit exceeded: max {_RATE_LIMIT_PER_HOUR} downloads/hour",
        )
    bucket.append(now)


def _purge_expired_downloads() -> None:
    now = time.time()
    try:
        for path in _DOWNLOAD_DIR.iterdir():
            if not path.is_file():
                continue
            try:
                age = now - path.stat().st_mtime
            except OSError:
                continue
            if age > _DOWNLOAD_TTL_SECONDS:
                try:
                    path.unlink(missing_ok=True)
                except OSError:
                    pass
    except FileNotFoundError:
        return


def _cache_key_for_url(raw_url: str) -> str:
    return hashlib.sha256(raw_url.encode("utf-8", errors="ignore")).hexdigest()


def _newpipe_resolve_json(raw_url: str) -> dict:
    if not _NEWPIPE_EXTRACTOR_URL:
        raise RuntimeError("NEWPIPE_EXTRACTOR_URL is not configured")

    endpoint = f"{_NEWPIPE_EXTRACTOR_URL}/v1/resolve_audio?url={quote(raw_url, safe='')}"
    req = Request(endpoint, method="GET", headers={"Accept": "application/json"})

    with urlopen(req, timeout=_NEWPIPE_EXTRACTOR_TIMEOUT) as resp:
        body = resp.read().decode("utf-8", errors="replace")

    data = json.loads(body)
    if not isinstance(data, dict):
        raise RuntimeError("NewPipe sidecar returned non-object JSON")
    if not data.get("success"):
        raise RuntimeError(str(data.get("error") or "NewPipe sidecar reported failure"))

    audio_url = data.get("audio_url")
    if not audio_url or not isinstance(audio_url, str):
        raise RuntimeError("NewPipe sidecar response missing audio_url")

    return data


def _guess_ext_from_mime(mime: str | None) -> str | None:
    if not mime:
        return None
    m = mime.split(";", 1)[0].strip().lower()
    return {
        "audio/mp4": ".m4a",
        "audio/mpeg": ".mp3",
        "audio/webm": ".webm",
        "audio/ogg": ".ogg",
        "audio/opus": ".opus",
        "audio/aac": ".aac",
        "audio/flac": ".flac",
        "audio/wav": ".wav",
    }.get(m)


def _guess_ext_from_url(audio_url: str) -> str | None:
    lower = audio_url.lower()
    if "mime=audio%2Fmp4" in lower or "mime=audio/mp4" in lower:
        return ".m4a"
    if "mime=audio%2Fwebm" in lower or "mime=audio/webm" in lower:
        return ".webm"
    if "mime=audio%2Fmpeg" in lower or "mime=audio/mpeg" in lower:
        return ".mp3"
    return None


def _http_download_to_file(*, audio_url: str, out_path: Path, timeout: float) -> tuple[str, str]:
    """
    Download bytes to disk. Returns (ext_without_dot, media_type).
    """
    # YouTube CDN URLs are picky; a browser-ish UA + Referer helps a lot.
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "*/*",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "close",
    }

    host = urlsplit(audio_url).netloc.lower()
    if "googlevideo.com" in host or "youtube.com" in host or "youtu.be" in host:
        headers["Referer"] = "https://www.youtube.com/"
        headers["Origin"] = "https://www.youtube.com"

    req = Request(audio_url, method="GET", headers=headers)
    with urlopen(req, timeout=timeout) as resp:
        content_type = (resp.headers.get("Content-Type") or "").split(";", 1)[0].strip()
        data = resp.read()

    ext = _guess_ext_from_mime(content_type) or _guess_ext_from_url(audio_url) or ".bin"
    media = {
        "m4a": "audio/mp4",
        "mp3": "audio/mpeg",
        "webm": "audio/webm",
        "opus": "audio/ogg",
        "ogg": "audio/ogg",
        "aac": "audio/aac",
        "flac": "audio/flac",
        "wav": "audio/wav",
    }.get(ext.lstrip("."), content_type or "application/octet-stream")

    out_path.write_bytes(data)
    return ext.lstrip("."), media


def _try_newpipe_http_download_to_cache(raw_url: str, cache_key: str) -> tuple[str, str, str] | None:
    if not _NEWPIPE_EXTRACTOR_URL:
        return None

    try:
        np = _newpipe_resolve_json(raw_url)
    except (HTTPError, URLError, TimeoutError, json.JSONDecodeError, RuntimeError) as e:
        logger.warning("[newpipe] resolve failed: %s", e)
        return None

    audio_url = str(np.get("audio_url") or "")
    if not audio_url:
        return None

    mime_hint = str(np.get("mime_type") or "")
    ext_from_np = _guess_ext_from_mime(mime_hint) or _guess_ext_from_url(audio_url) or ".bin"
    out_path = _DOWNLOAD_DIR / f"{cache_key}{ext_from_np}"

    try:
        ext, media = _http_download_to_file(
            audio_url=audio_url,
            out_path=out_path,
            timeout=max(_NEWPIPE_EXTRACTOR_TIMEOUT, 60.0),
        )
    except (HTTPError, URLError, TimeoutError) as e:
        logger.warning("[newpipe] http download failed: %s", e)
        try:
            out_path.unlink(missing_ok=True)
        except OSError:
            pass
        return None

    final_suffix = f".{ext}" if ext else out_path.suffix
    final_path = _DOWNLOAD_DIR / f"{cache_key}{final_suffix}"
    if final_path.resolve() != out_path.resolve():
        try:
            final_path.unlink(missing_ok=True)
        except OSError:
            pass
        try:
            out_path.replace(final_path)
        except OSError:
            # If rename fails, keep original path but note mismatch in logs.
            final_path = out_path

    logger.info(
        "[newpipe] downloaded via sidecar ext=%s media=%s title=%r uploader=%r itag=%s delivery=%r",
        ext,
        media,
        np.get("title"),
        np.get("uploader"),
        np.get("itag"),
        np.get("delivery_method"),
    )

    meta = {
        "cache_key": cache_key,
        "source_url": raw_url,
        "path": str(final_path),
        "ext": ext,
        "media_type": media,
        "created_at": time.time(),
        "source": "newpipe_extractor",
        "newpipe": {k: np.get(k) for k in ("title", "uploader", "duration", "itag", "codec", "bitrate", "mime_type", "delivery_method")},
    }
    (_DOWNLOAD_DIR / f"{cache_key}.meta.json").write_text(json.dumps(meta), encoding="utf-8")

    return f"{cache_key}{final_path.suffix}", final_path.name, media


def _download_audio_file_sync(raw_url: str) -> tuple[str, str, str]:
    """
    Returns (file_id, filename_on_disk, media_type).
    """
    cache_key = _cache_key_for_url(raw_url)

    if _NEWPIPE_EXTRACTOR_URL:
        fast = _try_newpipe_http_download_to_cache(raw_url, cache_key)
        if fast:
            return fast

    out_template = str(_DOWNLOAD_DIR / f"{cache_key}.%(ext)s")

    command = [
        sys.executable,
        "-m",
        "yt_dlp",
        "-f",
        "bestaudio/best",
        "--no-playlist",
        "--no-warnings",
        "--no-part",
        "-o",
        out_template,
        "--quiet",
        "--js-runtimes",
        "node",
        "--remote-components",
        "ejs:github",
        raw_url,
    ]

    proc = subprocess.run(
        command,
        capture_output=True,
        text=True,
        errors="replace",
    )
    if proc.returncode != 0:
        err = (proc.stderr or proc.stdout or "").strip()
        raise RuntimeError(err or f"yt-dlp failed (rc={proc.returncode})")

    candidates = sorted(
        _DOWNLOAD_DIR.glob(f"{cache_key}.*"),
        key=lambda p: p.stat().st_mtime if p.exists() else 0,
        reverse=True,
    )
    if not candidates:
        raise RuntimeError("yt-dlp finished but no output file was found")

    chosen = candidates[0]
    ext = chosen.suffix.lower().lstrip(".")
    media = {
        "m4a": "audio/mp4",
        "mp3": "audio/mpeg",
        "webm": "audio/webm",
        "opus": "audio/ogg",
        "ogg": "audio/ogg",
        "aac": "audio/aac",
        "flac": "audio/flac",
        "wav": "audio/wav",
    }.get(ext, "application/octet-stream")

    file_id = f"{cache_key}{chosen.suffix}"
    meta = {
        "cache_key": cache_key,
        "source_url": raw_url,
        "path": str(chosen),
        "ext": ext,
        "media_type": media,
        "created_at": time.time(),
        "source": "yt_dlp",
    }
    meta_path = _DOWNLOAD_DIR / f"{cache_key}.meta.json"
    meta_path.write_text(json.dumps(meta), encoding="utf-8")

    return file_id, chosen.name, media


async def _ensure_downloaded(raw_url: str, *, rate_limit_key: str | None = None) -> tuple[str, str, str]:
    """
    Queue + cache wrapper around synchronous yt-dlp download.
    """
    cache_key = _cache_key_for_url(raw_url)
    lock = _download_locks[cache_key]

    async with lock:
        existing = sorted(
            _DOWNLOAD_DIR.glob(f"{cache_key}.*"),
            key=lambda p: p.stat().st_mtime if p.exists() else 0,
            reverse=True,
        )
        for p in existing:
            if p.suffix.lower() == ".json":
                continue
            if p.is_file():
                age = time.time() - p.stat().st_mtime
                if age <= _DOWNLOAD_TTL_SECONDS:
                    ext = p.suffix.lower().lstrip(".")
                    media = {
                        "m4a": "audio/mp4",
                        "mp3": "audio/mpeg",
                        "webm": "audio/webm",
                        "opus": "audio/ogg",
                        "ogg": "audio/ogg",
                        "aac": "audio/aac",
                        "flac": "audio/flac",
                        "wav": "audio/wav",
                    }.get(ext, "application/octet-stream")
                    logger.info(
                        "[downloads] cache hit cache_key=%s file=%s",
                        cache_key,
                        p.name,
                    )
                    return f"{cache_key}{p.suffix}", p.name, media

        # Only enforce the per-IP hourly cap when we are about to spend real work
        # (yt-dlp / network download). Cached responses should not consume quota.
        if rate_limit_key is not None:
            _enforce_rate_limit(rate_limit_key)

        async with _download_semaphore:
            loop = asyncio.get_running_loop()
            return await loop.run_in_executor(
                _download_executor,
                lambda: _download_audio_file_sync(raw_url),
            )


def _resolve_youtube_cookiefile() -> tuple[str | None, str | None]:
    """
    Return a stable, writable cookie path and only refresh when source changes.
    """
    def _file_sig(path: str) -> tuple[int, int] | None:
        try:
            st = os.stat(path)
            return st.st_size, int(st.st_mtime)
        except OSError:
            return None

    def _choose_cookie_source() -> tuple[str | None, str | None]:
        # 1) ENV (Render/host secret file)
        env_path = os.environ.get("YOUTUBE_COOKIES_FILE", "").strip()
        if env_path and os.path.isfile(env_path):
            return env_path, "ENV"

        # 2) Local fallback
        local_path = _DEFAULT_COOKIES_FILE
        if local_path.is_file():
            return str(local_path), "local"

        return None, None

    source_path, source_label = _choose_cookie_source()
    now = time.time()
    if not source_path:
        global _cookie_missing_logged_at
        if now - _cookie_missing_logged_at >= _COOKIE_REFRESH_INTERVAL_SECONDS:
            logger.warning("[cookies] ❌ No cookies found")
            _cookie_missing_logged_at = now
        return None, None

    source_sig = _file_sig(source_path)
    if not source_sig:
        logger.warning("[cookies] Source cookie file is not readable: %s", source_path)
        return None, None

    global _cookie_cache_source, _cookie_cache_label, _cookie_cache_sig, _cookie_cache_last_check_ts
    with _cookie_cache_lock:
        cache_path_exists = os.path.isfile(_COOKIE_TMP_PATH)
        source_unchanged = _cookie_cache_source == source_path and _cookie_cache_sig == source_sig
        recently_checked = (now - _cookie_cache_last_check_ts) < _COOKIE_REFRESH_INTERVAL_SECONDS

        # Fast path: reuse copied cookie file if source unchanged.
        if source_unchanged and cache_path_exists and recently_checked:
            return _COOKIE_TMP_PATH, _cookie_cache_label

        should_copy = not source_unchanged or not cache_path_exists
        if should_copy:
            try:
                cookie_tmp_parent = os.path.dirname(_COOKIE_TMP_PATH)
                if cookie_tmp_parent:
                    os.makedirs(cookie_tmp_parent, exist_ok=True)
                shutil.copy2(source_path, _COOKIE_TMP_PATH)
                logger.info("[cookies] ✅ Synced %s cookies → %s", source_label, _COOKIE_TMP_PATH)
            except Exception as e:
                logger.warning("[cookies] ❌ Copy failed, using source directly: %s", e)
                _cookie_cache_last_check_ts = now
                return source_path, source_label

        _cookie_cache_source = source_path
        _cookie_cache_label = source_label
        _cookie_cache_sig = source_sig
        _cookie_cache_last_check_ts = now
        return _COOKIE_TMP_PATH, source_label


def _youtube_error_suggests_cookie_retry(msg: str) -> bool:
    """Datacenter / anonymous extraction often gets bot interstitials; cookies can help."""
    lower = msg.casefold()
    return any(
        needle in lower
        for needle in (
            "sign in to confirm",
            "not a bot",
            "cookies-from-browser",
            "cookies for the authentication",
        )
    )


def _yt_dlp_opts(
    *,
    skip_cookiefile: bool = False,
    cookie_path: str | None = None,
    cookie_source: str | None = None,
) -> dict:
    resolved_path = cookie_path
    resolved_source = cookie_source
    if resolved_path is None and resolved_source is None:
        resolved_path, resolved_source = _resolve_youtube_cookiefile()

    # 🔥 KEY FIX: Always allow mobile clients FIRST
    if skip_cookiefile:
        cookies_path = None
        player_client = ["android", "ios", "web"]
        logger.info("[yt-dlp] Using NO cookies (mobile clients)")
    else:
        cookies_path = resolved_path
        # 🔥 IMPORTANT: include mobile even WITH cookies
        player_client = ["android", "ios", "web"]
        if cookies_path:
            logger.info(
                "[yt-dlp] Using cookies + mobile clients (source=%s, path=%s)",
                resolved_source or "unknown",
                cookies_path,
            )
        else:
            logger.info("[yt-dlp] Cookies requested but unavailable; running without cookies")

    # Prefer audio-only; "best" can fail on some restricted/odd manifests.
    opts: dict = {
        "format": "bestaudio/best",
        "quiet": True,
        "no_warnings": True,
        "skip_download": True,
        "noplaylist": True,
        # Use Node.js for YouTube's JS challenges/signature solving (VPS/datacenter IPs).
        # Mirrors CLI: `yt-dlp --js-runtimes node ...`
        # Python API expects a dict: {runtime: {config}}
        "js_runtimes": {"node": {}},
        "extractor_args": {
            "youtube": {
                "player_client": player_client,
            },
        },
    }

    if cookies_path:
        opts["cookiefile"] = cookies_path

    return opts


def _info_dict_to_stream_result(info: dict) -> dict:
    """Pick a playable URL from yt-dlp format entries (avoid relying on top-level url)."""
    formats = info.get("formats", [])

    selected_fmt = None
    preferred_format_ids = ["140", "251", "250", "249"]

    for format_id in preferred_format_ids:
        for fmt in formats:
            if str(fmt.get("format_id") or fmt.get("itag") or "") == format_id and fmt.get("url"):
                selected_fmt = fmt
                break
        if selected_fmt:
            break

    if not selected_fmt:
        audio_only = [
            fmt for fmt in formats
            if fmt.get("url")
            and fmt.get("vcodec") in (None, "none")
            and fmt.get("acodec") not in (None, "none")
        ]

        if audio_only:
            m4a = [fmt for fmt in audio_only if str(fmt.get("ext") or "").lower() == "m4a"]
            webm = [fmt for fmt in audio_only if str(fmt.get("ext") or "").lower() == "webm"]

            pool = m4a or webm or audio_only
            selected_fmt = max(pool, key=lambda fmt: fmt.get("abr") or 0)

    audio_url = selected_fmt.get("url") if selected_fmt else None
    if not audio_url:
        raise ValueError("No audio-only stream URL found")

    if selected_fmt.get("vcodec") not in (None, "none") or selected_fmt.get("acodec") in (None, "none"):
        raise ValueError(
            "Rejected non-audio-only format: "
            f"format_id={selected_fmt.get('format_id')} "
            f"itag={selected_fmt.get('itag')} "
            f"vcodec={selected_fmt.get('vcodec')} "
            f"acodec={selected_fmt.get('acodec')}"
        )

    title = info.get("title") or "Unknown"
    artist = info.get("uploader") or info.get("artist") or "Unknown"
    duration = info.get("duration")

    logger.info(f"[get_audio_stream] Success! Title: {title}, Artist: {artist}")
    logger.info(f"[get_audio_stream] Audio URL (truncated): {audio_url[:80]}...")
    logger.info(
        "[get_audio_stream] Selected format: format_id=%s itag=%s ext=%s acodec=%s vcodec=%s abr=%s",
        selected_fmt.get("format_id") if selected_fmt else None,
        selected_fmt.get("itag") if selected_fmt else None,
        selected_fmt.get("ext") if selected_fmt else None,
        selected_fmt.get("acodec") if selected_fmt else None,
        selected_fmt.get("vcodec") if selected_fmt else None,
        selected_fmt.get("abr") if selected_fmt else None,
    )

    return {
        "audio_url": audio_url,
        "title": title,
        "artist": artist,
        "duration": duration,
    }


def _newpipe_dict_to_stream_result(np: dict) -> dict:
    audio_url = str(np.get("audio_url") or "")
    if not audio_url:
        raise ValueError("NewPipe sidecar returned empty audio_url")

    mime = str(np.get("mime_type") or "").lower()
    if mime.startswith("video/"):
        raise ValueError(f"Rejected NewPipe muxed/video mime_type={mime!r}")

    lower_url = audio_url.lower()
    if lower_url.endswith(".m3u8") or "manifest" in lower_url:
        # ExoPlayer may handle these, but the app stack is much simpler with progressive/DASH audio files.
        raise ValueError("Rejected NewPipe HLS/manifest-style audio_url")

    title = str(np.get("title") or "Unknown")
    artist = str(np.get("uploader") or "Unknown")
    duration = np.get("duration")

    logger.info("[get_audio_stream][newpipe] Success! Title: %s, Artist: %s", title, artist)
    logger.info("[get_audio_stream][newpipe] Audio URL (truncated): %s...", audio_url[:80])
    logger.info(
        "[get_audio_stream][newpipe] Selected stream: itag=%s codec=%s abr=%s mime=%s delivery=%s",
        np.get("itag"),
        np.get("codec"),
        np.get("bitrate"),
        np.get("mime_type"),
        np.get("delivery_method"),
    )

    return {
        "audio_url": audio_url,
        "title": title,
        "artist": artist,
        "duration": duration,
    }


def get_audio_stream(url: str) -> dict:
    """Extract audio stream URL and metadata (NewPipe sidecar first, then yt-dlp)."""
    logger.info(f"[get_audio_stream] Input URL: {url}")

    if _NEWPIPE_EXTRACTOR_URL:
        try:
            np = _newpipe_resolve_json(url)
            return _newpipe_dict_to_stream_result(np)
        except (HTTPError, URLError, TimeoutError, json.JSONDecodeError, RuntimeError, ValueError) as e:
            logger.warning("[get_audio_stream] NewPipe sidecar failed; falling back to yt-dlp: %s", e)

    cookie_path, cookie_source = _resolve_youtube_cookiefile()
    phases: list[tuple[bool, str]] = []
    if cookie_path:
        phases.append((True, "without cookies"))
    phases.append((False, "with cookies if configured"))

    last_download_error: DownloadError | None = None

    for skip_cookies, phase_label in phases:
        ydl_opts = _yt_dlp_opts(
            skip_cookiefile=skip_cookies,
            cookie_path=cookie_path,
            cookie_source=cookie_source,
        )
        active_cookie_path = ydl_opts.get("cookiefile")
        if active_cookie_path:
            logger.info(
                "[get_audio_stream] Cookie mode=ON (phase=%s, source=%s, path=%s)",
                phase_label,
                cookie_source or "unknown",
                active_cookie_path,
            )
        else:
            logger.info(
                "[get_audio_stream] Cookie mode=OFF (phase=%s, reason=%s)",
                phase_label,
                "explicit skip" if skip_cookies else "no cookie file available",
            )
        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                logger.info(
                    "[get_audio_stream] Extracting (%s) for: %s",
                    phase_label,
                    url,
                )
                info = ydl.extract_info(url, download=False)

            if not info:
                logger.error("[get_audio_stream] yt-dlp returned no info")
                raise ValueError("Failed to extract video info")

            result = _info_dict_to_stream_result(info)
            logger.info(
                "[get_audio_stream] Extraction success with cookies=%s (phase=%s)",
                "yes" if active_cookie_path else "no",
                phase_label,
            )
            return result

        except DownloadError as e:
            last_download_error = e
            msg = str(e)
            if (
                skip_cookies
                and cookie_path
                and _youtube_error_suggests_cookie_retry(msg)
            ):
                logger.warning(
                    "[get_audio_stream] %s… — retrying with cookies.",
                    msg[:160],
                )
                continue
            logger.exception(f"[get_audio_stream] Error extracting audio: {e}")
            raise

        except Exception as e:
            logger.exception(f"[get_audio_stream] Error extracting audio: {e}")
            raise

    if last_download_error:
        raise last_download_error
    raise RuntimeError("get_audio_stream: no extraction phase ran")


def get_ytmusic_synced_lyrics(title: str, artist: str) -> str | None:
    """
    Fetch synced lyrics (with timestamps) from YouTube Music for the given song/artist.
    Returns the raw lyrics string (with timestamps) or None if not available.
    """
    query = f"{title} {artist}".strip()
    if not query:
        return None

    logger.info(f"[ytmusic] Searching for: {query!r}")
    try:
        results = ytmusic_client.search(query, filter="songs")
    except Exception as e:
        logger.warning(f"[ytmusic] Search failed: {e}")
        return None

    if not results:
        logger.info("[ytmusic] No search results")
        return None

    video_id = results[0].get("videoId")
    if not video_id:
        logger.info("[ytmusic] First result has no videoId")
        return None

    logger.info(f"[ytmusic] Using videoId={video_id}")
    try:
        watch = ytmusic_client.get_watch_playlist(video_id)
    except Exception as e:
        logger.warning(f"[ytmusic] get_watch_playlist failed: {e}")
        return None

    lyrics_id = watch.get("lyrics")
    if not lyrics_id:
        logger.info("[ytmusic] No lyrics_id on watch playlist")
        return None

    logger.info(f"[ytmusic] Fetching lyrics for lyrics_id={lyrics_id}")
    try:
        lyrics_data = ytmusic_client.get_lyrics(lyrics_id, timestamps=True)
    except Exception as e:
        logger.warning(f"[ytmusic] get_lyrics failed: {e}")
        return None

    if not isinstance(lyrics_data, dict):
        logger.info("[ytmusic] get_lyrics returned non-dict")
        return None

    has_timestamps = lyrics_data.get("hasTimestamps")
    raw_lyrics_value = lyrics_data.get("lyrics")

    # Normalise lyrics value to a single string; ytmusicapi may return list/other.
    if isinstance(raw_lyrics_value, list):
        raw_lyrics = "\n".join(str(line) for line in raw_lyrics_value)
    elif isinstance(raw_lyrics_value, str):
        raw_lyrics = raw_lyrics_value
    else:
        raw_lyrics = str(raw_lyrics_value or "")

    # Only accept lyrics that have timestamps and non-empty content
    if not has_timestamps or not raw_lyrics.strip():
        logger.info(
            f"[ytmusic] Lyrics found but rejected "
            f"(has_timestamps={has_timestamps}, empty={not bool(raw_lyrics.strip())})"
        )
        return None

    # At this point we have timestamped lyrics in YouTube Music's "LyricLine(...)" style.
    # Convert them to LRC-style "[MM:SS.xx] line" so the frontend can parse it
    # exactly like Lrclib's `syncedLyrics`.
    def _ms_to_tag(ms: int) -> str:
        total_seconds = ms / 1000.0
        minutes = int(total_seconds // 60)
        seconds = int(total_seconds % 60)
        frac = int(round((total_seconds - minutes * 60 - seconds) * 100))
        return f"[{minutes:02d}:{seconds:02d}.{frac:02d}]"

    lines: list[str] = []
    # Each line looks roughly like:
    # LyricLine(text='...', start_time=13410, end_time=18630, id=1)
    pattern = re.compile(
        r"LyricLine\(text='(?P<text>.*?)', start_time=(?P<start>\d+), .*?\)"
    )
    for raw_line in str(raw_lyrics).splitlines():
        raw_line = raw_line.strip()
        if not raw_line:
            continue
        m = pattern.match(raw_line)
        if not m:
            # If it doesn't match the pattern, just skip; we only care about well-formed lines.
            continue
        text = m.group("text").strip()
        if not text:
            continue
        start_ms = int(m.group("start"))
        tag = _ms_to_tag(start_ms)
        lines.append(f"{tag} {text}")

    if not lines:
        logger.info("[ytmusic] No LyricLine entries parsed from lyrics")
        return None

    formatted = "\n".join(lines)
    logger.info("[ytmusic] Synced lyrics (with timestamps) found and formatted to LRC style")
    return formatted


@app.get("/")
def root():
    logger.info("[API] Root / requested")
    return {"message": "Server is running"}


@app.get("/api/audio")
def api_audio(url: str = Query(..., description="YouTube or other video URL")):
    logger.info(f"[API] /api/audio called with url param")
    raw_url = unquote(url)
    logger.info(f"[API] Decoded URL: {raw_url[:100]}...")

    try:
        result = get_audio_stream(raw_url)
        logger.info(f"[API] Returning audio stream for: {result.get('title', '?')}")
        logger.info("[API] /api/audio response: status=200 success=True")
        return {"success": True, **result}
    except Exception as e:
        logger.error(f"[API] Failed to get audio: {e}")
        logger.error("[API] /api/audio response: status=500 success=False detail=%s", str(e))
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/lyrics")
def api_lyrics(
    title: str = Query(..., description="Track title"),
    artist: str = Query(..., description="Artist name"),
):
    """
    Fetch synced lyrics (with timestamps) from YouTube Music.

    This endpoint is intended to be used as a *fallback* from the frontend
    when Lrclib does not return syncedLyrics.
    """
    logger.info(f"[API] /api/lyrics called with title={title!r}, artist={artist!r}")

    try:
        synced = get_ytmusic_synced_lyrics(title, artist)
        if not synced:
            logger.info("[API] /api/lyrics: no synced lyrics available from YouTube Music")
            logger.info("[API] /api/lyrics response: status=200 success=False (no synced lyrics)")
            # 200 with success=False so frontend can decide final fallback
            return {
                "success": False,
                "synced_lyrics": None,
                "detail": "No synced lyrics with timestamps found from YouTube Music",
            }

        logger.info("[API] /api/lyrics: returning synced lyrics from YouTube Music")
        logger.info("[API] /api/lyrics response: status=200 success=True")
        return {
            "success": True,
            "synced_lyrics": synced,
        }
    except Exception as e:
        logger.error(f"[API] /api/lyrics failed: {e}")
        logger.error("[API] /api/lyrics response: status=200 success=False detail=%s", str(e))
        # Do not hard-fail the app; just indicate failure
        return {
            "success": False,
            "synced_lyrics": None,
            "detail": str(e),
        }


_EXT_TO_MIME: dict[str, str] = {
    "m4a":  "audio/mp4",
    "mp3":  "audio/mpeg",
    "webm": "audio/webm",
    "ogg":  "audio/ogg",
    "opus": "audio/ogg",
    "aac":  "audio/aac",
    "flac": "audio/flac",
    "wav":  "audio/wav",
}


async def _download_handler(request: Request, url: str) -> dict:
    raw_url = unquote(url)
    client_ip = _client_ip(request)

    logger.info("[downloads] queued download for ip=%s url=%s", client_ip, raw_url[:120])
    try:
        file_id, filename, media_type = await _ensure_downloaded(
            raw_url,
            rate_limit_key=client_ip,
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("[downloads] download failed: %s", e)
        raise HTTPException(status_code=500, detail=str(e)) from e

    file_url = _build_file_url(request, file_id)
    logger.info(
        "[downloads] ready file_id=%s filename=%s media_type=%s external_url=%s",
        file_id,
        filename,
        media_type,
        "yes" if _EXTERNAL_FILE_BASE_URL else "no",
    )
    return {
        "success": True,
        "file_id": file_id,
        "filename": filename,
        "media_type": media_type,
        "file_url": file_url,
        "source_url": raw_url,
    }


@app.get("/api/download")
async def api_download(request: Request, url: str = Query(..., description="YouTube or other video URL")):
    return await _download_handler(request, url)


@app.get("/download")
async def download_alias(request: Request, url: str = Query(..., description="YouTube or other video URL")):
    return await _download_handler(request, url)


@app.get("/api/get_file/{file_id}")
async def api_get_file(file_id: str):
    path = (_DOWNLOAD_DIR / file_id).resolve()
    if not str(path).startswith(str(_DOWNLOAD_DIR)):
        raise HTTPException(status_code=400, detail="Invalid file id")
    if not path.is_file():
        raise HTTPException(status_code=404, detail="File not found")

    ext = path.suffix.lower().lstrip(".")
    media_type = _EXT_TO_MIME.get(ext, "application/octet-stream")
    return FileResponse(path, media_type=media_type, filename=path.name)


@app.get("/get_file/{file_id}")
async def get_file_alias(file_id: str):
    return await api_get_file(file_id)


if __name__ == "__main__":
    import uvicorn

    port = int(os.environ.get("PORT", "8000"))
    logger.info("[SERVER] Starting Lyrical Insta API on host 0.0.0.0 port %s", port)
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")
