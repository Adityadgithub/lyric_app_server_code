# YouTube audio backend (Python + yt-dlp)

This server provides the audio stream URL for the Flutter app. It uses **yt-dlp** (no YouTube API key required).

## Setup

```bash
cd backend
pip install -r requirements.txt
```

## Run

```bash
uvicorn server:app --reload --port 8765
```

Keep this running while using the Flutter app. The app calls `http://localhost:8765/audio_url?artist=...&title=...`.

## Optional: use pytubefix instead of yt-dlp

To use **pytubefix** instead, install it and adapt `server.py`:

```bash
pip install pytubefix
```

Then in `server.py` you can use `pytubefix.Search("artist title")` and get the first result’s audio stream URL (see pytubefix docs for stream selection).

## Mobile / emulator

- **Android emulator**: use `http://10.0.2.2:8765` as the base URL in the Flutter app (so the device can reach your machine’s localhost).
- **Physical device**: use your computer’s LAN IP, e.g. `http://192.168.1.x:8765`, and ensure the backend is reachable on the network.
