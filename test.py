from ytmusicapi import YTMusic
from urllib.parse import urlparse, parse_qs

yt = YTMusic()

# 🔥 Extract video ID from YouTube URL
def extract_video_id(url):
    parsed = urlparse(url)

    if parsed.hostname == "youtu.be":
        return parsed.path[1:]

    if parsed.hostname and "youtube.com" in parsed.hostname:
        return parse_qs(parsed.query).get("v", [None])[0]

    return None


def get_lyrics(song=None, artist=None, youtube_url=None):
    videoId = None

    # ✅ 1. Try YouTube URL first (NEW FEATURE)
    if youtube_url:
        videoId = extract_video_id(youtube_url)

    # ✅ 2. Fallback to search (your original logic)
    if not videoId and song and artist:
        results = yt.search(f"{song} {artist}", filter="songs")

        if not results:
            return None

        videoId = results[0]["videoId"]

    if not videoId:
        return None

    # ✅ 3. Fetch lyrics
    watch = yt.get_watch_playlist(videoId)
    lyrics_id = watch.get("lyrics")

    if not lyrics_id:
        return None

    lyrics_data = yt.get_lyrics(lyrics_id, timestamps=True)

    return lyrics_data


# 🔥 Usage examples

# 1️⃣ Using song + artist (old way)
# data = get_lyrics(song="Barsaat Mein", artist="Yo Yo Honey Singh")

# 2️⃣ Using YouTube URL (NEW - more accurate)
data = get_lyrics(youtube_url="https://youtu.be/9QTgJaxSXkk?si=0ucwVepMWdBohqQp")


if data:
    print("Has timestamps:", data.get("hasTimestamps"))
    print(data["lyrics"])
else:
    print("No lyrics found")