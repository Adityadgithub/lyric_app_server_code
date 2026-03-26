
"""
FastAPI server that:
- extracts audio stream URLs from YouTube/other video links using yt-dlp
- optionally fetches synced lyrics from YouTube Music (ytmusicapi)
"""
import logging
import os
import re
import shutil
from pathlib import Path
from urllib.parse import unquote

import subprocess

from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
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


# Global YTMusic client (unauthenticated; uses public endpoints)
ytmusic_client = YTMusic()

_BACKEND_DIR = Path(__file__).resolve().parent
_DEFAULT_COOKIES_FILE = _BACKEND_DIR / "cookies.txt"


def _resolve_youtube_cookiefile() -> tuple[str | None, str | None]:
    """
    Always return a writable cookie file path (Render-safe)
    """

    TMP_PATH = "/tmp/cookies.txt"

    def copy_to_tmp(src: str, label: str):
        try:
            shutil.copy2(src, TMP_PATH)
            logger.info(f"[cookies] ✅ Copied {label} → {TMP_PATH}")
            return TMP_PATH, label
        except Exception as e:
            logger.warning(f"[cookies] ❌ Copy failed: {e}")
            return src, label

    # 1. ENV (Render secret file)
    env_path = os.environ.get("YOUTUBE_COOKIES_FILE", "").strip()

    if env_path and os.path.isfile(env_path):
        logger.info("[cookies] Using ENV cookie file")
        return copy_to_tmp(env_path, "ENV")

    # 2. Local fallback
    local_path = _DEFAULT_COOKIES_FILE

    if local_path.is_file():
        logger.info("[cookies] Using local cookies.txt")
        return copy_to_tmp(str(local_path), "local")

    logger.warning("[cookies] ❌ No cookies found")
    return None, None


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


def _yt_dlp_opts(*, skip_cookiefile: bool = False) -> dict:
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
            logger.info("[yt-dlp] Using cookies + mobile clients")

    opts: dict = {
        "format": "best",
        "quiet": True,
        "no_warnings": True,
        "skip_download": True,
        "noplaylist": True,
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

    audio_url = None

    preferred_itags = [251, 250, 249, 140]

    for itag in preferred_itags:
        for fmt in formats:
            if fmt.get("itag") == itag and fmt.get("url"):
                audio_url = fmt["url"]
                break
        if audio_url:
            break

    if not audio_url:
        for fmt in formats:
            if fmt.get("vcodec") == "none" and fmt.get("acodec") != "none":
                audio_url = fmt.get("url")
                if audio_url:
                    break

    if not audio_url:
        for fmt in formats:
            if fmt.get("acodec") != "none":
                audio_url = fmt.get("url")
                if audio_url:
                    break

    if not audio_url and formats:
        audio_url = formats[0].get("url")

    if not audio_url:
        raise ValueError("No audio stream URL found")

    title = info.get("title") or "Unknown"
    artist = info.get("uploader") or info.get("artist") or "Unknown"
    duration = info.get("duration")

    logger.info(f"[get_audio_stream] Success! Title: {title}, Artist: {artist}")
    logger.info(f"[get_audio_stream] Audio URL (truncated): {audio_url[:80]}...")

    return {
        "audio_url": audio_url,
        "title": title,
        "artist": artist,
        "duration": duration,
    }


def get_audio_stream(url: str) -> dict:
    """Extract audio stream URL and metadata using yt-dlp."""
    logger.info(f"[get_audio_stream] Input URL: {url}")

    cookie_path, _ = _resolve_youtube_cookiefile()
    phases: list[tuple[bool, str]] = []
    if cookie_path:
        phases.append((True, "without cookies"))
    phases.append((False, "with cookies if configured"))

    last_download_error: DownloadError | None = None

    for skip_cookies, phase_label in phases:
        ydl_opts = _yt_dlp_opts(skip_cookiefile=skip_cookies)
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

            return _info_dict_to_stream_result(info)

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
        return {"success": True, **result}
    except Exception as e:
        logger.error(f"[API] Failed to get audio: {e}")
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
            # 200 with success=False so frontend can decide final fallback
            return {
                "success": False,
                "synced_lyrics": None,
                "detail": "No synced lyrics with timestamps found from YouTube Music",
            }

        logger.info("[API] /api/lyrics: returning synced lyrics from YouTube Music")
        return {
            "success": True,
            "synced_lyrics": synced,
        }
    except Exception as e:
        logger.error(f"[API] /api/lyrics failed: {e}")
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


def _pick_best_stream_format(url: str) -> tuple[str, str]:
    cookie_path, cookie_source = _resolve_youtube_cookiefile()

    def extract_with_clients(clients, use_cookies):
        opts: dict = {
            "quiet": True,
            "no_warnings": True,
            "skip_download": True,
            "noplaylist": True,
            "extractor_args": {
                "youtube": {"player_client": clients},
            },
        }
        if use_cookies and cookie_path:
            opts["cookiefile"] = cookie_path

        with yt_dlp.YoutubeDL(opts) as ydl:
            return ydl.extract_info(url, download=False)

    info = None

    # 🔥 TRY 1: MOBILE (MOST IMPORTANT — works on Render)
    try:
        logger.info("[stream-fmt] Trying mobile clients (no cookies)")
        info = extract_with_clients(["android", "ios"], False)
    except Exception as e:
        logger.warning(f"[stream-fmt] Mobile failed: {e}")

    # 🔥 TRY 2: WEB + COOKIES
    if not info and cookie_path:
        try:
            logger.info("[stream-fmt] Trying web + cookies")
            info = extract_with_clients(["web"], True)
        except Exception as e:
            logger.warning(f"[stream-fmt] Web+cookies failed: {e}")

    if not info:
        logger.warning("[stream-fmt] All extraction failed → fallback bestaudio")
        return "bestaudio", "audio/mp4"

    formats = info.get("formats", [])

    audio_only = [
        f for f in formats
        if f.get("vcodec") in (None, "none")
        and f.get("acodec") not in (None, "none")
        and f.get("url")
    ]

    logger.info(f"[stream-fmt] Found {len(audio_only)} audio formats")

    if not audio_only:
        return "bestaudio", "audio/mp4"

    # prefer m4a
    m4a = [f for f in audio_only if f.get("ext") == "m4a"]
    if m4a:
        best = max(m4a, key=lambda f: f.get("abr") or 0)
        return best["format_id"], "audio/mp4"

    # fallback webm
    webm = [f for f in audio_only if f.get("ext") == "webm"]
    if webm:
        best = max(webm, key=lambda f: f.get("abr") or 0)
        return best["format_id"], "audio/webm"

    best = max(audio_only, key=lambda f: f.get("abr") or 0)
    return best["format_id"], "audio/mp4"

@app.get("/api/stream")
def api_stream(url: str = Query(..., description="YouTube or other video URL")):
    """
    Stream audio directly to the client by piping yt-dlp stdout.

    Flutter's ExoPlayer hits this endpoint instead of a short-lived
    googlevideo.com URL, so there are no 403 / cookie / IP issues.
    """
    raw_url = unquote(url)
    logger.info("[API] /api/stream called for: %s", raw_url[:100])

    # Detect the best available audio-only format before spawning the subprocess
    format_id, mime_type = _pick_best_stream_format(raw_url)
    logger.info("[API] /api/stream → format_id=%s  mime=%s", format_id, mime_type)

    cookie_path, cookie_source = _resolve_youtube_cookiefile()

    command = [
        "yt-dlp",
        "-f", format_id,
        "--no-playlist",
        "--no-part",
        "-o", "-",
        "--quiet",
    ]
    if cookie_path:
        command += ["--cookies", cookie_path]
        logger.info("[API] /api/stream using cookiefile (%s)", cookie_source)

    command.append(raw_url)

    try:
        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
    except FileNotFoundError:
        raise HTTPException(
            status_code=500,
            detail="yt-dlp executable not found. Make sure it is installed and on PATH.",
        )
    except Exception as e:
        logger.error("[API] /api/stream failed to start yt-dlp: %s", e)
        raise HTTPException(status_code=500, detail=str(e))

    def _iter_chunks(proc: subprocess.Popen, chunk_size: int = 64 * 1024):
        try:
            while True:
                chunk = proc.stdout.read(chunk_size)
                if not chunk:
                    err = proc.stderr.read().decode(errors="replace")
                    if err.strip():
                        logger.error("[API] /api/stream yt-dlp stderr: %s", err)
                    break
                yield chunk
        finally:
            proc.stdout.close()
            proc.wait()
            logger.info("[API] /api/stream yt-dlp process finished (rc=%s)", proc.returncode)

    return StreamingResponse(
        _iter_chunks(process),
        media_type=mime_type,
        headers={
            "Accept-Ranges": "none",
            "Content-Disposition": "inline",
        },
    )


if __name__ == "__main__":
    import uvicorn

    port = int(os.environ.get("PORT", "8000"))
    logger.info("[SERVER] Starting Lyrical Insta API on host 0.0.0.0 port %s", port)
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")
