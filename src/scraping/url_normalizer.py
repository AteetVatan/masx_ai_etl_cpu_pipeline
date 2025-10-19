# ┌───────────────────────────────────────────────────────────────┐
# │  MASX AI — URL Normalizer                                     │
# └───────────────────────────────────────────────────────────────┘
from __future__ import annotations
from typing import Optional
from urllib.parse import urlparse, parse_qs, unquote
import re
import requests

DEFAULT_UA = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "Chrome/120.0.0.0 Safari/537.36"
)

# Safe schemes only
SAFE_SCHEMES = {"http", "https"}

# Match Google News RSS redirector pattern
_GOOGLE_NEWS_RE = re.compile(r"^https?://news\.google\.com/rss/articles/", re.I)


def _is_safe_url(url: str) -> bool:
    try:
        p = urlparse(url)
        return p.scheme in SAFE_SCHEMES and bool(p.netloc)
    except Exception:
        return False


def extract_continue_from_consent(url: str) -> str:
    parsed = urlparse(url)
    qs = parse_qs(parsed.query)
    if "continue" in qs:
        return unquote(qs["continue"][0])
    return url


def resolve_redirects(url: str, timeout: float = 6.0, max_hops: int = 5) -> str:
    """
    Follows HTTP redirects with a browser-like UA.
    Returns the final URL (or the original if unresolved).
    """
    if not _is_safe_url(url):
        return url

    session = requests.Session()
    session.headers.update({"User-Agent": DEFAULT_UA})
    try:
        # HEAD is faster but not all servers allow it; fall back to GET.
        resp = session.head(url, allow_redirects=True, timeout=timeout)

        continue_url = extract_continue_from_consent(resp.url)
        final = continue_url
        if final == url or not _is_safe_url(final):
            resp = session.get(url, allow_redirects=True, timeout=timeout)
            final = resp.url
        return final if _is_safe_url(final) else url
    except Exception as e:
        return url


def normalize_google_news(url: str) -> str:
    """
    If it's a Google News RSS redirector, resolve to publisher URL.
    Otherwise return unchanged.
    """
    if _GOOGLE_NEWS_RE.match(url):
        return resolve_redirects(url)
    return url


def normalize_url(url: str) -> str:
    """
    Entry point for all URL normalization (extensible later: t.co, bit.ly, etc.).
    """
    if not _is_safe_url(url):
        return url
    # 1) Google News redirector
    url = normalize_google_news(url)
    # 2) Future: add shortener unwrapping if needed (bit.ly, lnkd.in, etc.)
    return url
