import hashlib
import base64
import re

class URLUtils:

    @staticmethod
    def generate_unique_code(url: str, length: int = 6) -> str:
        """
        Generate a unique, deterministic short code from a URL.

        Args:
            url: Input URL string
            length: Desired code length (default 12 chars)

        Returns:
            A short alphanumeric hash code derived from the URL.
        """
        if not url:
            return "unknown"

        # Normalize the URL (remove query params and trailing slashes)
        normalized = re.sub(r"[?#].*", "", url.strip().lower().rstrip("/"))

        # Compute SHA-256 hash, then Base32-encode for compactness
        sha = hashlib.sha256(normalized.encode("utf-8")).digest()
        code = base64.b32encode(sha).decode("utf-8").rstrip("=").lower()

        # Trim to requested length
        return code[:length]
