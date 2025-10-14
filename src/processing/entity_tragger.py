from __future__ import annotations
from typing import Dict, Any, List, Iterable, Tuple
from collections import defaultdict, Counter
import re
from transformers import pipeline
from src.config import get_settings, get_service_logger
from src.models import EntityModel, EntityAttributes, MetaAttributes

# Optional: LOC→GPE promotion
try:
    import countrytagger  # provides tag_place, tag_text_countries
    _HAS_COUNTRYTAGGER = True
except Exception:
    countrytagger = None
    _HAS_COUNTRYTAGGER = False


class EntityTagger:
    """
    Multilingual entity extraction using DistilBERT (WikiANN) + light heuristics.

    - Base NER: PER / ORG / LOC (from the model)
    - GPE split: LOC that are sovereign countries via countrytagger.tag_place => GPE
    - Extra detectors: EVENT, LAW, DATE, MONEY, QUANTITY, NORP (regex/light lexicon)
    - CPU-only; handles 100k+ chars via paragraph-aware chunking
    """

    MODEL_ID = "Davlan/distilbert-base-multilingual-cased-ner-hrl"

    def __init__(self, chunk_chars: int = 20_000, batch_size: int = 16) -> None:
        self.settings = get_settings()
        self.logger = get_service_logger("EntityTagger")
        self.enabled = getattr(self.settings, "enable_entity_extraction", True)
        self.chunk_chars = max(5_000, int(chunk_chars))
        self.batch_size = max(1, int(batch_size))

        try:
            self.ner = pipeline(
                task="ner",
                model=self.MODEL_ID,
                device=-1,                      # CPU
                aggregation_strategy="simple",  # merge wordpieces
                batch_size=self.batch_size,
            )
            self.logger.info("NER model loaded", model=self.MODEL_ID)
        except Exception as e:
            self.logger.warning("NER model failed to load", error=str(e))
            self.ner = None
            
       
        if _HAS_COUNTRYTAGGER:
            self.logger.info("countrytagger available: LOC→GPE promotion enabled")
        else:
            self.logger.info("countrytagger not available: LOC→GPE promotion disabled")

        # ---- lightweight regex extractors ----
        self.re_year = re.compile(r"\b(1[5-9]\d{2}|20\d{2}|21\d{2})\b")
        self.re_date_num = re.compile(r"\b([0-3]?\d)[/\-.]([0-1]?\d)[/\-.]((?:19|20)\d{2})\b")
        self.re_money = re.compile(
            r"(?:(?<!\w)(?:R\$|US\$|\$|€|£|¥)\s?\d{1,3}(?:[\.\,\s]\d{3})*(?:[\,\.]\d{1,2})?"
            r"|(?:\d{1,3}(?:[\.\,\s]\d{3})*(?:[\,\.]\d{1,2})?\s?(?:USD|EUR|BRL|GBP|JPY)))"
        )
        self.re_quantity = re.compile(
            r"\b\d[\d\.\,\s]*\s?(?:km2|km²|km|m²|m3|MW|GW|kW|t|ton(?:nes)?|milhões|bilhões|%)\b",
            flags=re.IGNORECASE,
        )
        self.re_event = re.compile(
            r"\b(COP ?\d{1,2}|Protocolo\s+de\s+\w+|Acordo\s+de\s+\w+|Tratado\s+de\s+\w+|Summit|Cúpula)\b",
            flags=re.IGNORECASE,
        )
        self.re_law = re.compile(
            r"\b(Lei\s+[^\d\W]\w+|Lei\s+\d[\w\.\-\/]*|PL\s?\d[\w\.\-\/]*|MP\s?\d[\w\.\-\/]*|Decreto\s+\d[\w\.\-\/]*)",
            flags=re.IGNORECASE,
        )
        self.re_norp = re.compile(
            r"\b(indígenas?|democratas?|republicanos?|socialistas?|comunistas?|europeus?|brasileir[oa]s?)\b",
            flags=re.IGNORECASE,
        )

        # HF → canonical labels
        self.label_map = {"PER": "PERSON", "ORG": "ORG", "LOC": "LOC"}

    # ---- Public API ---------------------------------------------------------

    def extract_entities(self, text: str) -> EntityModel:
        try:
            
            if not self.enabled or not text or not self.ner:
                return EntityModel(
                    PERSON=[], ORG=[], GPE=[], LOC=[], NORP=[], EVENT=[], LAW=[],
                    DATE=[], MONEY=[], QUANTITY=[],
                    meta=MetaAttributes(
                        chunks=0, chars=len(text) if text else 0,
                        model=self.MODEL_ID, score=0.0
                    )
                )

            raw: Dict[str, List[Tuple[str, float]]] = defaultdict(list)
            num_chunks = 0

            # 1) NER per chunk
            for num_chunks, chunk in enumerate(self._iter_chunks(text), start=1):
                try:
                    results = self.ner(chunk)
                except Exception as e:
                    self.logger.warning("NER failed on chunk", error=str(e))
                    continue

                for ent in results or []:
                    label_raw = ent.get("entity_group") or ent.get("entity")
                    label = self.label_map.get(label_raw, None)
                    if not label:
                        continue
                    word = (ent.get("word") or "").strip()
                    if not word:
                        continue
                    score = float(ent.get("score") or 0.0)
                    raw[label].append((word, score))

            # ***Locations will be refined and handled by Geotagger***
            # 2) LOC → GPE promotion using countrytagger
            # promoted_gpe: List[Tuple[str, float]] = []
            # kept_loc: List[Tuple[str, float]] = []
            # for word, score in raw.get("LOC", []):
            #     if _HAS_COUNTRYTAGGER:
            #         try:
            #             code, cscore, alpha2 = countrytagger.tag_place(word)
            #             if code == "PCLI" and alpha2:
            #                 promoted_gpe.append((word, max(score, float(cscore or 0.0))))
            #                 continue
            #             # substring fallback (e.g., "Universidade de Brasília" → "Brasil")
            #             for a2, cscore2, _cname in countrytagger.tag_text_countries(word):
            #                 promoted_gpe.append((word, max(score, float(cscore2 or 0.0))))
            #                 break
            #             else:
            #                 kept_loc.append((word, score))
            #         except Exception:
            #             kept_loc.append((word, score))
            #     else:
            #         kept_loc.append((word, score))


            # 3) Aggregate & dedupe
            buckets: Dict[str, Dict[str, float]] = {"PERSON": {}, 
                                                    "ORG": {}, 
                                                    "GPE": {}, 
                                                    "LOC": {},
                                                    "NORP": {},
                                                    "EVENT": {},
                                                    "LAW": {},
                                                    "DATE": {}, 
                                                    "MONEY": {}, 
                                                    "QUANTITY": {}}

            def _accumulate(pairs: List[Tuple[str, float]], label: str) -> None:
                counts = Counter()
                scores = defaultdict(float)
                canonicals = {}

                for w, s in pairs:
                    if not w:
                        continue
                    # normalize for merging
                    merge_key = w.strip().lower()
                    if not merge_key:
                        continue

                    # pick a canonical form (first seen or title-case)
                    if merge_key not in canonicals:
                        canonicals[merge_key] = w.strip().title()

                    counts[merge_key] += 1
                    scores[merge_key] = max(scores[merge_key], s)

                for mk in counts:
                    canonical = canonicals[mk]
                    buckets[label][canonical] = round(scores[mk], 4)

            _accumulate(raw.get("PERSON", []), "PERSON")
            _accumulate(raw.get("ORG", []), "ORG")
            _accumulate(raw.get("LOC", []), "LOC")
            _accumulate([], "GPE")

            # 4) Regex layers
            extra = self._extract_extras(text)
            # for label in ["EVENT", "LAW", "DATE", "MONEY", "QUANTITY", "NORP"]:
            #     if label not in buckets:
            #         buckets[label] = {}
            #     for w, s in extra.get(label, []):
            #         buckets[label][w] = max(buckets[label].get(w, 0.0), s)
            for label in ["EVENT", "LAW", "DATE", "MONEY", "QUANTITY", "NORP"]:             
                _accumulate(extra.get(label, []), label)
                
            # 5) Build EntityModel
            def _list(mapping: Dict[str, float]) -> List[EntityAttributes]:
                items = [EntityAttributes(text=k, score=float(v)) for k, v in mapping.items()]
                items.sort(key=lambda e: (-e.score, e.text.lower()))
                return items

            all_scores = [v for m in buckets.values() for v in m.values()]
            overall = round(sum(all_scores) / len(all_scores), 4) if all_scores else 0.0

            return EntityModel(
                PERSON=_list(buckets.get("PERSON", {})),
                ORG=_list(buckets.get("ORG", {})),
                GPE=_list(buckets.get("GPE", {})),
                LOC=_list(buckets.get("LOC", {})),
                NORP=_list(buckets.get("NORP", {})),
                EVENT=_list(buckets.get("EVENT", {})),
                LAW=_list(buckets.get("LAW", {})),
                DATE=_list(buckets.get("DATE", {})),
                MONEY=_list(buckets.get("MONEY", {})),
                QUANTITY=_list(buckets.get("QUANTITY", {})),
                meta=MetaAttributes(
                    chunks=num_chunks, chars=len(text), model=self.MODEL_ID, score=overall
                )
            )
        except Exception as e:
            self.logger.warning("EntityTagger failed to extract entities", error=str(e))
            return EntityModel(
                PERSON=[], ORG=[], GPE=[], LOC=[], NORP=[], EVENT=[], LAW=[],
                DATE=[], MONEY=[], QUANTITY=[],
                meta=MetaAttributes(chunks=0, chars=len(text) if text else 0, model=self.MODEL_ID, score=0.0)
            )
    def _get_geopolitical_entities(self, text: str) -> set[str]:
        """
        Use spaCy multilingual NER (xx_ent_wiki_sm) to extract GPE/LOC entities
        from long text, chunked with _iter_chunks.
        Returns a set of lowercase entity names.
        """
        entities: set[str] = set()
        if not self.spacy_nlp:
            return entities
        
        
        """
    Run spaCy NER on input text and return a dynamic dictionary
    with all detected entity labels as keys.
    """

        if not self.spacy_nlp:
            raise RuntimeError("spaCy pipeline not initialized")

        results = defaultdict(list)
        chars = len(text)
        chunks = 0
        scores = []

        for chunk in self._iter_chunks(text):
            try:
                doc = self.spacy_nlp(chunk)
                chunks += 1
                for ent in doc.ents:
                    results[ent.label_].append(
                        {"text": ent.text, "score": 1.0}  # spaCy doesn't expose scores
                    )
                    scores.append(1.0)
            except Exception as e:
                self.logger.warning("spaCy failed on a chunk", error=str(e))
                continue

        avg_score = sum(scores) / len(scores) if scores else 0.0

        
             
        
        

        for chunk in self._iter_chunks(text):
            try:
                doc = self.spacy_nlp(chunk)
                for ent in doc.ents:
                    if ent.label_ in {"GPE"}:
                        entities.add(ent.text.lower())                        
            except Exception as e:
                self.logger.warning("spaCy failed on a chunk", error=str(e))
                continue        
        return entities

    # ---- Internals ----------------------------------------------------------

    def _iter_chunks(self, text: str) -> Iterable[str]:
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

    def _extract_extras(self, text: str) -> Dict[str, List[Tuple[str, float]]]:
        found: Dict[str, List[Tuple[str, float]]] = defaultdict(list)

        for m in self.re_event.finditer(text):
            val = m.group(0).strip()
            if val: found["EVENT"].append((val, 0.95))

        for m in self.re_law.finditer(text):
            val = m.group(0).strip()
            if val: found["LAW"].append((val, 0.90))

        for m in self.re_year.finditer(text):
            found["DATE"].append((m.group(0), 0.99))
        for m in self.re_date_num.finditer(text):
            found["DATE"].append((m.group(0), 0.97))

        for m in self.re_money.finditer(text):
            found["MONEY"].append((m.group(0), 0.95))

        for m in self.re_quantity.finditer(text):
            found["QUANTITY"].append((m.group(0), 0.90))

        for m in self.re_norp.finditer(text):
            found["NORP"].append((m.group(0), 0.85))

        # dedupe per label
        for label, items in list(found.items()):
            mx = defaultdict(float)
            for w, s in items:
                mx[w] = max(mx[w], s)
            found[label] = [(k, round(v, 4)) for k, v in mx.items()]
        return found


# Singleton and module-level API
entity_tagger = EntityTagger()
