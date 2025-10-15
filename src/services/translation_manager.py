# translation_manager.py

from __future__ import annotations
import time, threading
from typing import List, Dict, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import OrderedDict
from random import choice
import requests
import os
from deep_translator import (
    GoogleTranslator,  # keyless (scrapes web)
    PapagoTranslator,  # keyless (web), good for Asian langs
    MyMemoryTranslator,  # keyless dictionary
    LingueeTranslator,  # keyless dictionary
)


from src.utils import LanguageUtils


# --- Configs ---
DEFAULT_TARGET = "en"
FAIL_THRESHOLD = 3
COOLDOWN_SEC = 120
MAX_WORKERS = 4
CACHE_SIZE = 100_000  # in-memory LRU entries
REQUEST_TIMEOUT = 12.0

from enum import Enum


class Providers(Enum):
    GOOGLE = "google"
    MYMEMORY = "mymemory"
    FREEAPI = "freeapi"


from deep_translator.constants import MY_MEMORY_LANGUAGES_TO_CODES


class LanguageNotSupportedException(Exception):
    pass


class LengthNotSupportedException(Exception):
    pass


class Circuit:
    def __init__(self):
        self.fail_count = 0
        self.open_until = 0.0
        self.lock = threading.Lock()

    def allow(self) -> bool:
        with self.lock:
            return time.time() >= self.open_until

    def success(self):
        with self.lock:
            self.fail_count = 0
            self.open_until = 0.0

    def failure(self):
        with self.lock:
            self.fail_count += 1
            if self.fail_count >= FAIL_THRESHOLD:
                self.open_until = time.time() + COOLDOWN_SEC


class TranslationManager:
    _instance = None
    _global_lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        with cls._global_lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(
        self,
        target_lang: str = DEFAULT_TARGET,
        enable_google: bool = True,
        enable_mymemory: bool = True,
        enable_freeapi: bool = True,
        timeout_sec: float = REQUEST_TIMEOUT,
    ):
        if hasattr(self, "_initialized"):
            return
        self._initialized = True

        self.target = target_lang
        self.timeout = timeout_sec

        # providers on/off
        self.providers_enabled: Dict[Providers, bool] = {
            Providers.GOOGLE: enable_google,
            Providers.MYMEMORY: enable_mymemory,
            Providers.FREEAPI: enable_freeapi,
        }

        # circuits & locks
        self.circuits: Dict[Providers, Circuit] = {
            k: Circuit() for k in self.providers_enabled
        }
        self.providers_lock = threading.Lock()

        # LRU cache
        self._cache: "OrderedDict[Tuple[str,str,str], str]" = OrderedDict()
        self._cache_lock = threading.Lock()

    # ------------------------- public -------------------------
    async def translate(
        self,
        text: str,
        source: str = "auto",
        target: Optional[str] = None,
        proxies: Optional[List[str]] = None,
    ) -> str:
        target = target or self.target
        key = (text.strip(), source, target)

        # proxy= choice(proxies)
        # if proxy:
        #     os.environ["http_proxy"] = f"http://{proxy}"
        #     os.environ["https_proxy"] = f"http://{proxy}"

        os.environ["http_proxy"] = ""
        os.environ["https_proxy"] = ""

        cached = self._cache_get(key)
        if cached is not None:
            return cached

        for prov in self._provider_order():
            if not self._is_enabled(prov):
                continue
            if not self.circuits[prov].allow():
                continue
            try:
                res = await self._try_provider(prov, text, source, target)
                if res:
                    self._cache_put(key, res)
                    return res
            except Exception as e:
                last_exc = e
                continue
        return None

    def _detect_language_iso_code(self, text: str) -> str:
        return LanguageUtils.detect_language(text)

    def _get_supported_language_dict(self, klass):
        # {arabic: ar, french: fr, if
        return klass.get_supported_languages(as_dict=True) or {}

    def _get_language_code_for_provider(
        self, provider: Providers, source: str, target: str
    ) -> str:
        converted_source = None
        converted_target = None

        if provider == Providers.MYMEMORY:
            # mymemory supports BCP-47 Language Codes
            language_code_list = list(MY_MEMORY_LANGUAGES_TO_CODES.values())
            source, target = source.lower(), target.lower()
            for lang in language_code_list:
                # Match by prefix before the dash
                if lang.lower().startswith(source + "-"):
                    converted_source = lang
                if lang.lower().startswith(target + "-"):
                    converted_target = lang
                if converted_source and converted_target:
                    break
            return converted_source, converted_target

        return converted_source, converted_target

    # --------------------- internal core ----------------------
    async def _try_provider(
        self, provider: Providers, text: str, source: str = "auto", target: str = "en"
    ) -> Optional[str]:
        """
        Resolves the language of the text and the target language.
        source and target are iso_639 codes
        """
        try:
            if source == "auto" or source.strip() == "":
                source = self._detect_language_iso_code(text)

            # valtdate source and target
            if not LanguageUtils.is_valid_iso_639_code(
                source
            ) or not LanguageUtils.is_valid_iso_639_code(target):
                raise RuntimeError(
                    f"Invalid language ISO 639 code: {source} or {target}"
                )

            if provider == Providers.GOOGLE:
                gt = GoogleTranslator(source=source, target=target)
                language_dict = gt.get_supported_languages(as_dict=True) or {}
                language_code_list = list(language_dict.values())
                if source not in language_code_list or target not in language_code_list:
                    raise LanguageNotSupportedException(
                        f"Unsupported language: {source} or {target}"
                    )

                out = gt.translate(text)

            elif provider == Providers.MYMEMORY:
                if len(text) > 500:
                    raise LengthNotSupportedException(
                        f"Length not supported: {len(text)}"
                    )
                source, target = self._get_language_code_for_provider(
                    Providers.MYMEMORY, source, target
                )
                if not source or not target:
                    raise LanguageNotSupportedException(
                        f"Unsupported language: {source} or {target}"
                    )

                tr = MyMemoryTranslator(source=source, target=target)
                out = tr.translate(text)

            elif provider == Providers.FREEAPI:
                out = self._freeapi_translate(text, source, target)
            else:
                return None

            if not out or not isinstance(out, str):
                raise RuntimeError(f"{provider} returned empty/non-string result")

            self._mark_success(provider)
            return out
        except LanguageNotSupportedException as lns:
            return None
        except LengthNotSupportedException as lns:
            return None
        except Exception as e:
            self._mark_failure(provider)
            if not self.circuits[provider].allow():
                self._disable(provider)  # circuit opened → disable for session
            return None

    def _freeapi_translate(self, text: str, source: str, target: str) -> str:
        base = "https://ftapi.pythonanywhere.com/translate"
        params = {"dl": target, "text": text}
        if source and source.lower() != "auto":
            params["sl"] = source

        try:
            sess = requests.Session()

            # safe timeout instead of attribute (sess.timeout does not exist)
            resp = sess.get(base, params=params, timeout=self.timeout)

            # raise HTTP errors (4xx/5xx)
            resp.raise_for_status()

            # parse JSON safely
            try:
                data = resp.json()
            except ValueError as e:
                return None

            out = data.get("destination-text") or data.get("translated-text")
            if not out:
                return None

            return out.strip()

        except Exception as e:
            return None

    # --------------------- ordering / utils -------------------
    def _provider_order(self) -> List[Providers]:
        # make this order random everytime

        import random

        order = []
        if self.providers_enabled.get(Providers.GOOGLE):
            order.append(Providers.GOOGLE)
        if self.providers_enabled.get(Providers.FREEAPI):
            order.append(Providers.FREEAPI)
        if self.providers_enabled.get(Providers.MYMEMORY):
            order.append(Providers.MYMEMORY)
        random.shuffle(order)
        return order

    def _is_enabled(self, provider: Providers) -> bool:
        with self.providers_lock:
            return self.providers_enabled.get(provider, False)

    def _disable(self, provider: Providers) -> None:
        with self.providers_lock:
            self.providers_enabled[provider] = False

    def _mark_success(self, provider: Providers) -> None:
        self.circuits[provider].success()

    def _mark_failure(self, provider: Providers) -> None:
        self.circuits[provider].failure()

    # ----------------------- LRU cache ------------------------
    def _cache_get(self, key: Tuple[str, str, str]) -> Optional[str]:
        with self._cache_lock:
            val = self._cache.get(key)
            if val is not None:
                # move to end (recently used)
                self._cache.move_to_end(key, last=True)
            return val

    def _cache_put(self, key: Tuple[str, str, str], value: str) -> None:
        with self._cache_lock:
            self._cache[key] = value
            self._cache.move_to_end(key, last=True)
            # evict oldest
            if len(self._cache) > CACHE_SIZE:
                self._cache.popitem(last=False)


if __name__ == "__main__":
    tm = TranslationManager(
        target_lang="en",
        enable_google=True,
        enable_papago=True,
        enable_mymemory=True,
        enable_linguee=True,
        enable_freeapi=True,
        timeout_sec=12.0,
    )

    for txt in [
        "Élections en France reportées",
        "日本の首相が辞任",
        "Bundestag Haushalt Abstimmung",
        "Haus",  # word-level → Pons/Linguee may help
    ]:
        try:
            print(txt, "->", tm.translate(txt))
        except Exception as e:
            print("Failed:", txt, "error:", e)
