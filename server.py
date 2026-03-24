"""
FastAPI server that:
- extracts audio stream URLs from YouTube/other video links using yt-dlp
- optionally fetches synced lyrics from YouTube Music (ytmusicapi)
"""
import logging
import re
from urllib.parse import unquote

from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import yt_dlp
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


def get_audio_stream(url: str) -> dict:
    """Extract audio stream URL and metadata using yt-dlp."""
    logger.info(f"[get_audio_stream] Input URL: {url}")

    ydl_opts = {
        "format": "bestaudio/best",
        "quiet": True,
        "no_warnings": True,
        "skip_download": True,
        "noplaylist": True,
    }

    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            logger.info(f"[get_audio_stream] Extracting info for: {url}")
            info = ydl.extract_info(url, download=False)

            if not info:
                logger.error("[get_audio_stream] yt-dlp returned no info")
                raise ValueError("Failed to extract video info")

            # Get direct URL (from selected format)
            audio_url = info.get("url")
            if not audio_url and info.get("formats"):
                for fmt in info.get("formats", []):
                    if fmt.get("url") and fmt.get("vcodec") == "none":
                        audio_url = fmt["url"]
                        break
                if not audio_url and info["formats"]:
                    audio_url = info["formats"][0].get("url")

            if not audio_url:
                logger.error("[get_audio_stream] No audio URL found in info")
                raise ValueError("No audio stream URL found")

            title = info.get("title") or "Unknown"
            artist = info.get("uploader") or info.get("artist") or "Unknown"
            duration = info.get("duration")  # seconds, for lyrics matching

            logger.info(f"[get_audio_stream] Success! Title: {title}, Artist: {artist}")
            logger.info(f"[get_audio_stream] Audio URL (truncated): {audio_url[:80]}...")

            return {
                "audio_url": audio_url,
                "title": title,
                "artist": artist,
                "duration": duration,
            }
    except Exception as e:
        logger.exception(f"[get_audio_stream] Error extracting audio: {e}")
        raise


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
    return {"message": "Lyrical Insta API", "docs": "/docs", "audio": "GET /api/audio?url=..."}


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


if __name__ == "__main__":
    import uvicorn
    logger.info("[SERVER] Starting Lyrical Insta API on http://0.0.0.0:5000")
    uvicorn.run(app, host="0.0.0.0", port=5000, log_level="info")
