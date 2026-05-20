import importlib
import sys

# Prevent UnicodeEncodeError on Windows consoles when this package prints emojis.
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

yt_cookies = importlib.import_module("yt-cookies")

headers = {
    "Cookie": yt_cookies.youtube(),
}

print("Cookie loaded, length:", len(headers["Cookie"]))