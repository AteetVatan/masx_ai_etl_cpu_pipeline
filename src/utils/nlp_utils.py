import spacy
from src.config import get_service_logger


class NlpUtils:
    def __init__(self):
        self.logger = get_service_logger("NlpUtils")

        try:
            self.nlp = spacy.load("xx-ent-wiki-sm")
        except Exception:
            import xx_ent_wiki_sm
            self.nlp = xx_ent_wiki_sm.load()

        # Ensure sentence splitting is available
        if "sentencizer" not in self.nlp.pipe_names:
            self.nlp.add_pipe("sentencizer")

    def split_sentences(self, text: str) -> list[str]:
        """Split text into sentences safely across all models."""
        doc = self.nlp(text)
        return [sent.text.strip() for sent in doc.sents]
