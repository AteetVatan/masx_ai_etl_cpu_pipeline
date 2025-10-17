"""
Geotagging module for MASX AI ETL CPU Pipeline.

Uses CountryTagger (Aho–Corasick over GeoNames aliases) for multilingual COUNTRY detection.
City extraction is BONUS ONLY (via countrytagger.tag_place) and is OFF by default.

Designed for very long texts (500–100,000+ tokens) on CPU.

Entry point: extract_geographic_entities(text: str) -> dict
"""

from __future__ import annotations
from typing import Dict, Any, List, Iterable
from collections import OrderedDict, defaultdict

import countrytagger  # <-- module with tag_text_countries / tag_place
import pycountry
from src.models.geo_entity import GeoEntity
from src.models.entity_model import EntityAttributes

from src.config import get_settings, get_service_logger


class Geotagger:
    """
    Multilingual geotagging using CountryTagger (module-level API only).
    - Country = must (multilingual aliases via GeoNames alternate names)
    - City = bonus (available only via explicit tag_place calls; OFF by default)
    - CPU-only; scalable to 100k+ tokens with paragraph-aware chunking
    """

    def __init__(self, chunk_chars: int = 20_000) -> None:
        self.settings = get_settings()
        self.logger = get_service_logger("Geotagger")
        self.enabled = getattr(self.settings, "enable_geotagging", True)
        # never let chunk size be too small; large-enough windows reduce overhead and cross-boundary splits
        self.chunk_chars = max(5_000, int(chunk_chars))

        # CountryTagger builds/loads its Aho–Corasick automaton lazily on first call.
        # No object to construct; just confirm import:
        self.logger.info(
            "CountryTagger ready",
            version=getattr(countrytagger, "__version__", "unknown"),
            chunk_chars=self.chunk_chars,
        )

    # ---- public API ---------------------------------------------------------

    def extract_geographic_entities(
        self, title: str, text: str, locations: List[EntityAttributes]
    ) -> list[GeoEntity]:
        """
        Extract country (required) and optional city (bonus) entities from multilingual text.

        Args:
            text: raw text (500–100,000+ tokens supported)

        Returns:
            {
              "countries": [
                 {"name": "Germany", "alpha2": "DE", "count": 7, "avg_score": 0.90},
                 ...
              ],
              "cities": [],  # (bonus: stays empty unless city_bonus flow is enabled)
              "extraction_method": "countrytagger.tag_text_countries",
              "model_used": f"countrytagger {__version__}",
              "meta": {"chunks": int, "chars": int}
            }
        """
        try:
            if not self.enabled or not text:
                return []

            countries_dict, num_chunks = self._get_countrytragger_countries(text)
            (
                countries_from_title_dict,
                num_chunks_from_title,
            ) = self._get_countrytragger_countries(title)

            # BONUS: cities (off by default). This library maps place names -> country code,
            # but does not emit city names during scanning. If you want a minimal bonus pass,
            # toggle enable_city_bonus=True and provide your own candidate city strings.
            cities: List[Dict[str, Any]] = []

            # countries , countries_from_title -> take top 4
            # now merge them

            for (
                country_title_key,
                country_title_value,
            ) in countries_from_title_dict.items():
                if country_title_key not in countries_dict:
                    countries_dict[country_title_key] = country_title_value
                else:
                    countries_dict[country_title_key]["count"] += country_title_value[
                        "count"
                    ]
                    countries_dict[country_title_key]["avg_score"] = 1.0
                    if (
                        countries_dict[country_title_key]["avg_score"]
                        < country_title_value["avg_score"]
                    ):
                        countries_dict[country_title_key][
                            "avg_score"
                        ] = country_title_value["avg_score"]

            # Convert dict to list
            countries = list(countries_dict.values())

            # filter countries
            countries = [
                c for c in countries if c["count"] >= 2 and c["avg_score"] >= 0.6
            ]

            # --- spaCy validation (chunk-aware) ---
            if countries and locations:
                validated_loc_entities = self._validate_loc_entities_with_countrytagger(
                    locations
                )
                loc_codes = {
                    alpha2 for alpha2, score in validated_loc_entities
                }  # only ISO2 codes
                validated = [c for c in countries if c["alpha2"].lower() in loc_codes]
                if validated:
                    countries = validated

            # sort
            final_countries = sorted(
                countries, key=lambda d: (-d["count"], -d["avg_score"])
            )

            # take top 4
            final_countries = countries[:4]

            # final countries
            final_countries = [GeoEntity(**c) for c in final_countries]
            return final_countries

        except Exception as e:
            self.logger.warning(
                "Geotagger failed to extract geographic entities", error=str(e)
            )
            return []

    def _get_countrytragger_countries(
        self, text: str
    ) -> (Dict[str, Dict[str, Any]], int):
        try:
            # Accumulators (preserve stable order of first appearance)
            by_key: OrderedDict[str, Dict[str, Any]] = OrderedDict()
            counts = defaultdict(int)
            score_sums = defaultdict(float)
            num_chunks = 0
            for num_chunks, chunk in enumerate(self._iter_chunks(text), start=1):
                try:
                    # countrytagger.tag_text_countries yields (alpha2_code, score, country_name)
                    for (
                        feature_code,
                        score,
                        country,
                    ) in countrytagger.tag_text_countries(chunk):
                        if not country:
                            continue
                        key = country.lower()
                        enriched = self.enrich_country(key)

                        if key not in by_key:
                            by_key[key] = {
                                "name": enriched["name"],
                                "alpha2": enriched["alpha2"],
                                "alpha3": enriched["alpha3"],
                                "count": 0,
                                "avg_score": 0.0,
                            }
                        counts[key] += 1
                        score_sums[key] += float(score or 0.0)
                except Exception as e:
                    # Soft-fail per chunk; continue next
                    self.logger.warning("CountryTagger failed on a chunk", error=str(e))

            # finalize aggregates
            for k, row in by_key.items():
                row["count"] = counts[k]
                total = counts[k] or 1
                row["avg_score"] = round(score_sums[k] / total, 4)

            countries = sorted(by_key.values(), key=lambda d: (-d["count"], d["name"]))

            # Make it a dict with alpha2 as key
            countries_dict: Dict[str, Dict[str, Any]] = {
                country["alpha2"]: country for country in countries
            }

            return countries_dict, num_chunks

        except Exception as e:
            self.logger.warning("CountryTagger failed on a chunk", error=str(e))
            return [], 0

    # entiries set[str]
    def _validate_loc_entities_with_countrytagger(
        self, loc_entities: List[EntityAttributes]
    ) -> set[str]:
        valid_entities = set()
        # check entites against countrytagger.tag_place(ent)
        for loc in loc_entities:
            if (
                loc.score < 0.80
            ):  # from Davlan/distilbert-base-multilingual-cased-ner-hrl
                continue  # skip low confidence entities

            # 1. Exact match
            code, score, alpha2 = countrytagger.tag_place(loc.text)
            if code == "PCLI" and alpha2:
                combined_score = float(loc.score) * float(score or 0.0)

                valid_entities.add((alpha2.lower(), combined_score))
                continue

            # 2. Substring match
            for alpha2, score, country_name in countrytagger.tag_text_countries(
                loc.text
            ):
                if pycountry.countries.get(alpha_2=alpha2.upper()):
                    combined_score = float(loc.score) * float(score or 0.0)
                    valid_entities.add((alpha2.lower(), combined_score))

        return valid_entities

    # ---- internals ----------------------------------------------------------

    def _iter_chunks(self, text: str) -> Iterable[str]:
        """Yield ~chunk_chars-sized pieces on paragraph boundaries to avoid splitting names."""
        if len(text) <= self.chunk_chars:
            yield text
            return

        acc, acc_len = [], 0
        for line in text.splitlines(keepends=True):
            if acc_len + len(line) > self.chunk_chars and acc:
                yield "".join(acc)
                acc, acc_len = [], 0
            acc.append(line)
            acc_len += len(line)
        if acc:
            yield "".join(acc)

    # --- optional bonus helper (explicit use only) ---------------------------

    def tag_place(self, place_name: str) -> Dict[str, Any]:
        """
        BONUS helper: classify a single place string into a country using countrytagger.tag_place.
        Returns empty mapping if not recognized.
        """
        try:
            code, score, country_name = countrytagger.tag_place(place_name)
        except Exception as e:
            self.logger.warning("CountryTagger.tag_place failed", error=str(e))
            return {}
        if not code and not country_name:
            return {}
        return {
            "query": place_name,
            "alpha2": code,
            "country": country_name,
            "score": float(score or 0.0),
        }

    def enrich_country(self, iso2: str) -> dict:
        try:
            c = pycountry.countries.get(alpha_2=iso2.upper())
            if c:
                return {"name": c.name, "alpha2": c.alpha_2, "alpha3": c.alpha_3}
        except Exception:
            pass
        return {"name": iso2.upper(), "alpha2": iso2.upper(), "alpha3": None}
