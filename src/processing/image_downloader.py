"""
Supabase Image Downloader for MASX AI ETL CPU Pipeline.

Downloads images from ExtractResult and stores them in a Supabase Storage
bucket under:  <bucket>/<flashpoint_id>/<img_{n}_{extract_id}.{ext}>

- Concurrency-limited async downloads (httpx)
- Size & type guards
- Deterministic filenames
- Public or signed URLs returned (config)
"""

from __future__ import annotations
from typing import List, Dict, Any, Optional, Tuple
from io import BytesIO
from urllib.parse import urlparse
from url_normalize import url_normalize
import asyncio
import os
import posixpath
import mimetypes
import re
import imghdr
from urllib.parse import urlparse, urlunparse
from src.utils import URLUtils

import httpx
from supabase import create_client, Client

from src.config import get_settings, get_service_logger
from src.models import ExtractResult  # expects .id: str, .images: List[str]

logger = get_service_logger(__name__)
settings = get_settings()


class ImageDownloader:
    """
    Downloads images and saves them into Supabase storage bucket.
    """
    
    IMAGE_EXTENSIONS = (
        ".jpg", ".jpeg", ".png", ".gif", ".webp",
        ".avif", ".svg", ".bmp", ".tiff", ".tif", ".ico", ".heic", ".heif"
    )
    
    # Regex pattern: matches any of the extensions (case-insensitive)
    IMAGE_EXT_PATTERN = re.compile(
        r"\.(?:jpg|jpeg|png|gif|webp|avif|svg|bmp|tiff?|ico|heic|heif)\b",
        re.IGNORECASE
    )

    def __init__(self) -> None:
        """Initialize Supabase client and configs."""
        self.enabled: bool = getattr(settings, "enable_image_download", True)

        self.supabase_url: str = settings.supabase_url
        self.supabase_key: str = settings.supabase_service_role_key
        self.bucket_name: str = settings.supabase_image_bucket

        # Whether to return public or signed URL
        self.use_signed_urls: bool = getattr(settings, "supabase_use_signed_urls", False) # remove this property
        self.signed_url_expires_in: int = getattr(settings, "supabase_signed_url_expiry", 60 * 60)  # 1h

        # Network & safety
        self.timeout = httpx.Timeout(12.0)  # seconds
        self.max_concurrency: int = getattr(settings, "image_download_max_concurrency", 4)
        self.max_file_size: int = getattr(settings, "image_download_max_bytes", 5 * 1024 * 1024)  # 5MB
        self.allowed_schemes = {"http", "https"}

        # MIME & extension handling
        self.allowed_mime_prefixes = ("image/",)
        self.default_ext = ".jpg"

        # Create supabase client
        self.client: Client = create_client(self.supabase_url, self.supabase_key)
        self.storage = self.client.storage

        logger.info("ImageDownloader initialized")

    # ----------------- public API -----------------

    async def download_images(self, date: str, flashpoint_id: str, extracted_data: ExtractResult) -> ExtractResult:
        """
        Download images to Supabase Storage and update extracted_data.images
        to point at Supabase URLs (public or signed per config).

        Folder layout: <bucket>/<date>/<flashpoint_id>/
        File name:     img_{i}_{extract_id}.{ext}

        Returns:
            ExtractResult (same instance) with .images rewritten to Supabase URLs.
            Adds optional attribute: .image_upload_map (list of dict with source->dest)
        """
        if not self.enabled:
            logger.info("Image downloading disabled via config; skipping.")
            return extracted_data

        urls: List[str] = getattr(extracted_data, "images", []) or []
        
        
        urls = [self._clean_image_url(url) for url in urls]
        
        
        if not urls:
            logger.info("No images on ExtractResult; nothing to download.")
            return extracted_data

        extract_id = getattr(extracted_data, "id", "unknown")
        path = f"{date}/{flashpoint_id.strip()}"
        sem = asyncio.Semaphore(self.max_concurrency)
        
        # Clear all files in the directory
        self._clear_directory(path)

        async with httpx.AsyncClient(timeout=self.timeout, follow_redirects=True) as session:
            tasks = []
            for i, url in enumerate(urls):
                filename = self._build_filename(i, extract_id, url)
                bucket_path = posixpath.join(path, filename)
                tasks.append(self._process_one(session, url, bucket_path, sem))

            results: List[Tuple[str, Optional[str], Optional[str]]] = await asyncio.gather(*tasks, return_exceptions=False)

        # Build result lists
        uploaded_urls: List[str] = []
        image_map: List[Dict[str, Any]] = []

        for src_url, stored_path, served_url in results:
            
            if stored_path and served_url:
                uploaded_urls.append(served_url)
                image_map.append({"source": src_url, "bucket_path": stored_path, "served_url": served_url})
            else:
                # on failure, keep original URL to avoid emptying images entirely
                #uploaded_urls.append(src_url)
                image_map.append({"source": src_url, "bucket_path": None, "served_url": None})

        # Mutate ExtractResult in-place in a conservative way
        try:
            extracted_data.images = uploaded_urls
        except Exception:
            logger.warning("Could not assign uploaded URLs back to ExtractResult.images")

        # Non-breaking debug attachment (ok if the model has no such field)
        try:
            setattr(extracted_data, "image_upload_map", image_map)
        except Exception:
            pass

        return extracted_data
    
    def _clear_directory(self, path: str):
        """
        Clear all files in a directory on Supabase.
        """
        try:
            existing_files = self.storage.from_(self.bucket_name).list(path)
            if existing_files:
                for file_info in existing_files:
                    if isinstance(file_info, dict) and 'name' in file_info:
                        file_path = posixpath.join(path, file_info['name'])
                        self.storage.from_(self.bucket_name).remove([file_path])
                    elif isinstance(file_info, str):
                        file_path = posixpath.join(path, file_info)
                        self.storage.from_(self.bucket_name).remove([file_path])
                logger.info(f"Cleared {len(existing_files)} existing files from {path}")
        except Exception as e:
            logger.warning(f"Could not clear existing files from {path}: {e}")

    # ----------------- internal helpers -----------------

    async def _process_one(
        self,
        session: httpx.AsyncClient,
        url: str,
        bucket_path: str,
        sem: asyncio.Semaphore,
    ) -> Tuple[str, Optional[str], Optional[str]]:
        """
        Download a single image URL and upload to Supabase.

        Returns:
            (source_url, stored_bucket_path or None, served_url or None)
        """
        async with sem:
            try:
                if not self._is_url_allowed(url):
                    logger.warning(f"Blocked non-http(s) URL: {url}")
                    return (url, None, None)

                # HEAD first (if server supports) to check content-length & type
                content_type, content_length = await self._head_probe(session, url)

                if content_length is not None and content_length > self.max_file_size:
                    logger.warning(f"Skip {url}: Content-Length={content_length} > {self.max_file_size}")
                    return (url, None, None)

                data, content_type = await self._download_bytes(session, url, content_type_hint=content_type)
                if data is None:
                    return (url, None, None)
                
                
                # --- Validate MIME and actual image bytes ---
                # Quick MIME filter
                if not content_type or "image" not in content_type.lower():
                    logger.warning(f"Skip {url}: not an image MIME ({content_type})")
                    return (url, None, None)
                
                # Magic header test
                if not imghdr.what(None, h=data[:32]):
                    logger.warning(f"Skip {url}: invalid image header")
                    return (url, None, None)
                
                

                if len(data) > self.max_file_size:
                    logger.warning(f"Skip {url}: downloaded size {len(data)} > {self.max_file_size}")
                    return (url, None, None)

                if not self._is_mime_allowed(content_type):
                    logger.warning(f"Skip {url}: disallowed content-type={content_type}")
                    return (url, None, None)

                # Upload
                stored_path = await self._upload_bytes(bucket_path, data, content_type)
                if not stored_path:
                    return (url, None, None)

                # Public/signed URL
                served_url = self._serve_url(stored_path)
                return (url, stored_path, served_url)
            except Exception as e:
                logger.error(f"Failed processing image {url}: {e}", exc_info=False)
                return (url, None, None)

    async def _download_bytes(
        self,
        session: httpx.AsyncClient,
        url: str,
        content_type_hint: Optional[str] = None,
        timeout: float = 15.0,
    ) -> Tuple[Optional[bytes], str]:
        """
        GET the image safely. Returns (bytes or None, content_type).
        """
        r = await session.get(url, timeout=timeout)
        r.raise_for_status()

        content_type = r.headers.get("content-type") or content_type_hint or self._guess_mime_from_url(url) or "application/octet-stream"
        data = r.content  # small cap enforced by self.max_file_size
        return data, content_type

    async def _head_probe(
        self, session: httpx.AsyncClient, url: str
    ) -> Tuple[Optional[str], Optional[int]]:
        """Attempt HEAD first, fallback to GET/Range, then GET full if needed."""
        # 1. Try HEAD
        try:
            r = await session.head(url)
            if 200 <= r.status_code < 400:
                ctype = r.headers.get("content-type")
                clen = r.headers.get("content-length")
                clen_int = int(clen) if clen and clen.isdigit() else None
                return ctype, clen_int
        except Exception:
            pass

        # 2. Fallback: GET with Range
        try:
            r = await session.get(url, headers={"Range": "bytes=0-0"})
            if r.status_code in (200, 206):  # 206 = Partial Content
                ctype = r.headers.get("content-type")
                clen = r.headers.get("content-length")
                clen_int = int(clen) if clen and clen.isdigit() else None
                return ctype, clen_int
        except Exception:
            pass

        # 3. Last fallback: GET full (not ideal but guarantees compatibility)
        try:
            r = await session.get(url)
            if 200 <= r.status_code < 400:
                ctype = r.headers.get("content-type")
                clen = r.headers.get("content-length")
                clen_int = int(clen) if clen and clen.isdigit() else None
                return ctype, clen_int
        except Exception:
            pass

        return None, None


    def _build_filename(self, index: int, extract_id: str, url: str) -> str:
        """
        Returns deterministic filename: img_{index}_{extract_id}.{ext}
        """
        ext = self._guess_ext(url)
        safe_extract = "".join(ch for ch in str(extract_id) if ch.isalnum())[:32] or "x"
        safe_extract += URLUtils.generate_unique_code(url)
        return f"img_{index}_{safe_extract}{ext}"

    def _guess_ext(self, url: str) -> str:
        path = urlparse(url).path
        base, ext = os.path.splitext(path)
        ext = ext.lower()
        if ext and ext in self.IMAGE_EXTENSIONS:
            return ext
        # fallback by mime guess
        guessed = mimetypes.guess_extension(self._guess_mime_from_url(url) or "") or self.default_ext
        # Normalize .jpe to .jpg
        return ".jpg" if guessed in (".jpe",) else guessed

    def _guess_mime_from_url(self, url: str) -> Optional[str]:
        return mimetypes.guess_type(url)[0]

    def _is_url_allowed(self, url: str) -> bool:
        try:
            p = urlparse(url)
            return p.scheme.lower() in self.allowed_schemes
        except Exception:
            return False

    def _is_mime_allowed(self, content_type: Optional[str]) -> bool:
        if not content_type:
            return False
        return any(content_type.startswith(prefix) for prefix in self.allowed_mime_prefixes)
    
    def _clean_image_url(self, url: str) -> str:
        """
        Clean CMS-style or extended image URLs by truncating after the first valid extension.

        Example:
            https://site.com/foo.jpg/@@images/abc.png -> https://site.com/foo.jpg
        """
        url = url_normalize(url)
        parsed = urlparse(url)
        path = parsed.path

        # Find first occurrence of a known image extension
        match = self.IMAGE_EXT_PATTERN.search(path)
        if match:
            # truncate path right after the extension
            path = path[: match.end()]

        # rebuild normalized url
        clean_parts = (
            parsed.scheme or "https",
            parsed.netloc,
            path,
            "", "", ""  # drop params, query, fragment
        )
        return urlunparse(clean_parts)

    async def _upload_bytes(self, bucket_path: str, data: bytes, content_type: str) -> Optional[str]:
        """
        Upload to Supabase Storage. Returns stored path or None.
        Uses upsert=True to be idempotent across retries.
        """
        try:
            # Note: "folders" are virtual; uploading to a path creates it.
            file_options = {
                'content-type': content_type,
                'upsert': 'true',
                'cache-control': 'public, max-age=31536000'
                }
            resp = self.storage.from_(self.bucket_name).upload(
                path=bucket_path,
                file=data,
                file_options=file_options,
            )
            # supabase-py returns {'path': '...'} on success
            if isinstance(resp, dict) and "path" in resp:
                return resp["path"]
            # some versions return None on success; assume path is what we sent
            return bucket_path
        except Exception as e:
            logger.error(f"Upload failed for {bucket_path}: {e}")
            return None

    def _serve_url(self, stored_path: str) -> str:
        """
        Return a URL (public or signed) for the stored object.
        """
        try:
            bucket = self.storage.from_(self.bucket_name)
            if self.use_signed_urls:
                # create_signed_url(path, expires_in) -> {'signedURL': '...'}
                signed = bucket.create_signed_url(stored_path, self.signed_url_expires_in)
                if isinstance(signed, dict) and "signedURL" in signed:
                    return signed["signedURL"]
            # default to public
            pub = bucket.get_public_url(stored_path)
            if isinstance(pub, dict) and "publicUrl" in pub:
                return pub["publicUrl"]
            # some SDKs return string directly
            if isinstance(pub, str):
                return pub
        except Exception as e:
            logger.warning(f"Could not generate served URL for {stored_path}: {e}")

        # Fallback: construct typical public URL pattern (works if bucket is public)
        return f"{self.supabase_url}/storage/v1/object/public/{self.bucket_name}/{stored_path}"
