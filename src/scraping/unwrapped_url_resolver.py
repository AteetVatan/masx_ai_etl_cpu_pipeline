# file: get_final_url.py
import re
from urllib.parse import unquote
import httpx

ABS_URL = re.compile(r"https?://[^\s<>'\"()]+", re.IGNORECASE)

def _deep_unquote(s: str, rounds: int = 6) -> str:
    for _ in range(rounds):
        s2 = unquote(s)
        if s2 == s:
            break
        s = s2
    return s

def get_final_url(url_like: str, timeout: float = 20.0) -> str:
    """
    Returns the final URL after following redirects.
    Works even if `url_like` is a Google/Bing/DDG/search wrapper or percent-encoded.
    """
    s = _deep_unquote(url_like.strip().strip("'\""))
    m = ABS_URL.search(s)
    if not m:
        return url_like.strip()

    seed = m.group(0).strip("'\"")
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/124 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        # Helps with Google News tokens sometimes; harmless elsewhere
        "Referer": "https://news.google.com/",
        "Accept-Language": "en-US,en;q=0.8",
    }

    # Follow real redirects with GET (more reliable than HEAD)
    with httpx.Client(follow_redirects=True, timeout=timeout, headers=headers) as c:
        r = c.get(seed)
        return str(r.url)
