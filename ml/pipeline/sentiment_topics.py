from __future__ import annotations

import json
import logging
import math
import random
import re
from collections import Counter, OrderedDict
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd
from langdetect import DetectorFactory, detect
from tqdm.auto import tqdm

try:
    import torch
except ImportError:  # pragma: no cover - optional dependency
    torch = None  # type: ignore

try:
    import spacy
    from spacy.language import Language
except ImportError as exc:  # pragma: no cover
    raise RuntimeError("spaCy is required for sentiment_topics pipeline") from exc

DetectorFactory.seed = 42
random.seed(42)
np.random.seed(42)
if torch is not None:  # pragma: no branch
    torch.manual_seed(42)
    if torch.cuda.is_available():  # pragma: no branch
        torch.cuda.manual_seed_all(42)

LOGGER = logging.getLogger(__name__)

DEFAULT_COUNTRY_LANGUAGE_MAP: Dict[str, str] = {
    "fr": "fr",
    "us": "en",
    "de": "de",
    "se": "sv",
    "gb": "en",
    "ca": "en",
    "it": "it",
    "es": "es",
}

LANGUAGE_FALLBACKS: Dict[str, List[str]] = {
    "se": ["sv", "en"],
    "sv": ["sv", "en"],
    "ca": ["en"],
    "pt": ["pt", "en"],
    "mx": ["es", "en"],
}

SPACY_MODEL_NAMES: Dict[str, str] = {
    "en": "en_core_web_sm",
    "fr": "fr_core_news_sm",
    "de": "de_core_news_sm",
    "sv": "sv_core_news_sm",
    "it": "it_core_news_sm",
    "es": "es_core_news_sm",
    "ca": "ca_core_news_sm",
    "pt": "pt_core_news_sm",
}

_SENTIMENT_PIPELINE = None
_SENTIMENT_PIPELINE_DEVICE = None
_KEYBERT_MODEL = None


def _resolve_language(language: str, country_map: Dict[str, str]) -> str:
    lang = (language or "").lower()
    if not lang:
        return "en"
    if lang in SPACY_MODEL_NAMES:
        return lang
    # try mapping from countries
    if lang in country_map:
        return country_map[lang]
    fallbacks = LANGUAGE_FALLBACKS.get(lang, [])
    for fb in fallbacks:
        if fb in SPACY_MODEL_NAMES:
            return fb
    if len(lang) == 2:
        return lang
    return "en"


def detect_languages(df: pd.DataFrame, country_map: Optional[Dict[str, str]] = None) -> pd.Series:
    """Detect languages for each review row.

    Priority: explicit `language` column -> country map -> langdetect fallback.
    """

    if df.empty:
        raise ValueError("Input dataframe has no rows for language detection")

    country_map = country_map or DEFAULT_COUNTRY_LANGUAGE_MAP

    def _detect(row: pd.Series) -> str:
        lang_val = row.get("language")
        if isinstance(lang_val, str) and lang_val.strip():
            return lang_val.strip().lower()
        country = (row.get("country") or "").lower()
        if country in country_map:
            return country_map[country]
        text = row.get("cleaned_content") or row.get("content") or ""
        text = str(text).strip()
        if not text:
            return "en"
        try:
            return detect(text)
        except Exception:
            return "en"

    languages = df.apply(_detect, axis=1)
    return languages.astype(str)


def _ensure_sentencizer(nlp: Language) -> Language:
    if "senter" not in nlp.pipe_names and "sentencizer" not in nlp.pipe_names:
        try:
            nlp.add_pipe("sentencizer")
        except ValueError:
            pass
    return nlp


def load_spacy_models(
    languages: Iterable[str],
    country_map: Optional[Dict[str, str]] = None,
) -> Tuple[Dict[str, Language], Dict[str, str]]:
    """Load spaCy models for the provided languages with graceful fallback."""

    models: Dict[str, Language] = {}
    resolved_map: Dict[str, str] = {}
    country_map = country_map or DEFAULT_COUNTRY_LANGUAGE_MAP

    for lang in sorted(set(languages)):
        resolved = _resolve_language(lang, country_map)
        resolved_map[lang] = resolved
        if resolved in models:
            continue
        model_name = SPACY_MODEL_NAMES.get(resolved)
        loaded = None
        if model_name:
            try:
                loaded = spacy.load(model_name, disable=["ner", "lemmatizer", "tagger"])
                LOGGER.info("Loaded spaCy model %s for language %s", model_name, resolved)
            except Exception as exc:
                LOGGER.warning("Failed to load spaCy model %s (%s). Falling back to blank model.", model_name, exc)
        if loaded is None:
            try:
                loaded = spacy.blank(resolved)
            except Exception:
                loaded = spacy.blank("xx")
        models[resolved] = _ensure_sentencizer(loaded)
    return models, resolved_map


def split_sentences(
    df: pd.DataFrame,
    models: Dict[str, Language],
    lang_resolution: Dict[str, str],
    text_column: str = "cleaned_content",
) -> pd.DataFrame:
    """Split reviews into per-sentence rows."""

    required = {"id", "app_name", "country", text_column, "detected_language"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"split_sentences missing required columns: {missing}")

    rows: List[Dict[str, object]] = []

    for _, review in df.iterrows():
        lang = review["detected_language"]
        resolved = lang_resolution.get(lang, _resolve_language(lang, DEFAULT_COUNTRY_LANGUAGE_MAP))
        nlp = models.get(resolved) or models.get("en") or _ensure_sentencizer(spacy.blank("xx"))
        text = str(review[text_column] or "").strip()
        if not text:
            continue
        doc = nlp(text)
        sentences = [sent.text.strip() for sent in doc.sents if sent.text.strip()]
        if not sentences:
            sentences = [text]
        for idx, sentence in enumerate(sentences):
            rows.append(
                {
                    "id": review["id"],
                    "app_name": review["app_name"],
                    "country": review["country"],
                    "language": lang,
                    "resolved_language": resolved,
                    "sentence_index": idx,
                    "sentence": sentence,
                    "rating": review.get("rating"),
                    "review_date": review.get("review_date"),
                }
            )
    return pd.DataFrame(rows)


def _get_sentiment_pipeline(device: int):
    global _SENTIMENT_PIPELINE, _SENTIMENT_PIPELINE_DEVICE
    if _SENTIMENT_PIPELINE is not None and _SENTIMENT_PIPELINE_DEVICE == device:
        return _SENTIMENT_PIPELINE

    try:
        from transformers import pipeline

        LOGGER.info("Loading sentiment pipeline on device %s", device)
        _SENTIMENT_PIPELINE = pipeline(
            "sentiment-analysis",
            model="cardiffnlp/twitter-roberta-base-sentiment-latest",
            tokenizer="cardiffnlp/twitter-roberta-base-sentiment-latest",
            device=device,
            return_all_scores=True,
        )
        _SENTIMENT_PIPELINE_DEVICE = device
        return _SENTIMENT_PIPELINE
    except Exception as exc:  # pragma: no cover - fallback path
        LOGGER.warning("Falling back to rule-based sentiment due to: %s", exc)

        class _RuleBased:
            labels = ["negative", "neutral", "positive"]

            def __call__(self, texts: Sequence[str]):
                results = []
                for text in texts:
                    text_lower = text.lower()
                    score = 0
                    if any(tok in text_lower for tok in ["love", "great", "amazing", "good"]):
                        score += 1
                    if any(tok in text_lower for tok in ["hate", "bad", "terrible", "crash"]):
                        score -= 1
                    if score > 0:
                        results.append(
                            [
                                {"label": "negative", "score": 0.05},
                                {"label": "neutral", "score": 0.15},
                                {"label": "positive", "score": 0.8},
                            ]
                        )
                    elif score < 0:
                        results.append(
                            [
                                {"label": "negative", "score": 0.8},
                                {"label": "neutral", "score": 0.15},
                                {"label": "positive", "score": 0.05},
                            ]
                        )
                    else:
                        results.append(
                            [
                                {"label": "negative", "score": 0.2},
                                {"label": "neutral", "score": 0.6},
                                {"label": "positive", "score": 0.2},
                            ]
                        )
                return results

        _SENTIMENT_PIPELINE = _RuleBased()
        _SENTIMENT_PIPELINE_DEVICE = device
        return _SENTIMENT_PIPELINE


def run_sentiment(
    sentences: pd.DataFrame,
    batch_size: int = 32,
    device: Optional[int] = None,
) -> pd.DataFrame:
    """Run sentence-level sentiment analysis using a multilingual model."""

    if "sentence" not in sentences.columns:
        raise ValueError("sentences dataframe must include a 'sentence' column")

    if device is None:
        if torch is not None and torch.cuda.is_available():  # pragma: no branch
            device = 0
        else:
            device = -1

    pipe = _get_sentiment_pipeline(device)

    outputs: List[Dict[str, object]] = []
    iterator = range(0, len(sentences), batch_size)
    for start in tqdm(iterator, desc="Sentiment", unit="batch"):
        batch = sentences.iloc[start : start + batch_size]
        texts = batch["sentence"].astype(str).tolist()
        if not texts:
            continue
        predictions = pipe(texts)
        for (idx, row), scores in zip(batch.iterrows(), predictions):
            score_map = {item["label"].lower(): float(item["score"]) for item in scores}
            label = max(score_map, key=score_map.get)
            outputs.append(
                {
                    "index": idx,
                    "sentiment_label": label,
                    "positive": score_map.get("positive", 0.0),
                    "negative": score_map.get("negative", 0.0),
                    "neutral": score_map.get("neutral", 0.0),
                }
            )
    sentiment_df = pd.DataFrame(outputs).set_index("index")
    return sentiment_df


def aggregate_sentiment(labels: Sequence[str]) -> Tuple[str, float]:
    """Aggregate sentence-level labels into review-level label and score."""

    pos = sum(1 for label in labels if label == "positive")
    neg = sum(1 for label in labels if label == "negative")
    total = pos + neg
    if total == 0:
        return "neutral", 0.0
    pos_ratio = pos / total
    if 0.4 <= pos_ratio <= 0.6:
        label = "mixed"
    elif pos_ratio > 0.6:
        label = "positive"
    else:
        label = "negative"
    score = (pos - neg) / total
    return label, score


def _language_stopwords(language: str) -> List[str]:
    lang = (language or "en").lower()
    stopwords: List[str] = []
    try:
        if lang == "en":
            from spacy.lang.en.stop_words import STOP_WORDS as EN_STOP_WORDS

            stopwords = list(EN_STOP_WORDS)
        elif lang == "fr":
            from spacy.lang.fr.stop_words import STOP_WORDS as FR_STOP_WORDS

            stopwords = list(FR_STOP_WORDS)
        elif lang == "de":
            from spacy.lang.de.stop_words import STOP_WORDS as DE_STOP_WORDS

            stopwords = list(DE_STOP_WORDS)
        elif lang == "it":
            from spacy.lang.it.stop_words import STOP_WORDS as IT_STOP_WORDS

            stopwords = list(IT_STOP_WORDS)
        elif lang == "es":
            from spacy.lang.es.stop_words import STOP_WORDS as ES_STOP_WORDS

            stopwords = list(ES_STOP_WORDS)
        elif lang == "sv":
            from spacy.lang.sv.stop_words import STOP_WORDS as SV_STOP_WORDS

            stopwords = list(SV_STOP_WORDS)
    except Exception:
        stopwords = []
    additional = {"ui", "ux", "app", "apps", "application", "game"}
    return sorted(set(stopwords) | additional)


def _normalize_topic(topic: str, stopwords: Sequence[str]) -> Optional[str]:
    clean = topic.lower()
    clean = re.sub(r"[\"'`]+", "", clean)
    clean = re.sub(r"[^\w\s]", " ", clean)
    tokens = [tok for tok in clean.split() if tok not in stopwords]
    if not tokens:
        return None
    return " ".join(tokens)


def _simple_topic_fallback(sentences: Sequence[str], stopwords: Sequence[str], ngram_range: Tuple[int, int]) -> List[List[str]]:
    topics_per_sentence: List[List[str]] = []
    for sentence in sentences:
        tokens = [tok for tok in re.findall(r"\w+", sentence.lower()) if tok not in stopwords]
        ngrams: List[str] = []
        min_n, max_n = ngram_range
        for n in range(min_n, max_n + 1):
            for i in range(len(tokens) - n + 1):
                ngram = " ".join(tokens[i : i + n])
                ngrams.append(ngram)
        counter = Counter(ngrams)
        top = [phrase for phrase, _ in counter.most_common(3) if phrase]
        topics_per_sentence.append(top)
    return topics_per_sentence


def _get_keybert():
    global _KEYBERT_MODEL
    if _KEYBERT_MODEL is not None:
        return _KEYBERT_MODEL
    try:
        from keybert import KeyBERT
        from sentence_transformers import SentenceTransformer

        model = SentenceTransformer("all-MiniLM-L6-v2")
        _KEYBERT_MODEL = KeyBERT(model=model)
        return _KEYBERT_MODEL
    except Exception as exc:  # pragma: no cover - offline fallback
        LOGGER.warning("Falling back to simple topic extraction: %s", exc)
        _KEYBERT_MODEL = False  # type: ignore
        return _KEYBERT_MODEL


def extract_topics(
    sentences: Sequence[str],
    language: str,
    options: Optional[Dict[str, object]] = None,
) -> List[List[str]]:
    """Extract topics per sentence using KeyBERT with graceful fallback."""

    options = options or {}
    top_n = int(options.get("top_n", 5))
    ngram_range = options.get("ngram_range", (1, 2))
    diversity = float(options.get("diversity", 0.5))
    stopwords = _language_stopwords(language)

    keybert_model = _get_keybert()
    topics_per_sentence: List[List[str]] = []

    if keybert_model is False:
        return _simple_topic_fallback(sentences, stopwords, ngram_range)

    for sentence in sentences:
        text = sentence.strip()
        if not text:
            topics_per_sentence.append([])
            continue
        if keybert_model:
            try:
                keywords = keybert_model.extract_keywords(
                    text,
                    keyphrase_ngram_range=ngram_range,
                    stop_words=stopwords,
                    top_n=top_n,
                    use_maxsum=True,
                    diversity=diversity,
                )
                normalized = []
                seen = set()
                for phrase, score in keywords:
                    norm = _normalize_topic(phrase, stopwords)
                    if not norm or norm in seen:
                        continue
                    seen.add(norm)
                    normalized.append(norm)
                topics_per_sentence.append(normalized)
                continue
            except Exception as exc:  # pragma: no cover - fallback per sentence
                LOGGER.debug("KeyBERT failed for sentence '%s': %s", text[:50], exc)
        topics_per_sentence.append(
            [topic for topic in _simple_topic_fallback([text], stopwords, ngram_range)[0] if topic]
        )
    return topics_per_sentence


def merge_topics(topics: Sequence[Sequence[str]], limit: int = 5) -> List[str]:
    """Merge sentence-level topics into ordered unique review topics."""

    merged = OrderedDict()
    for sentence_topics in topics:
        for topic in sentence_topics:
            if topic and topic not in merged:
                merged[topic] = None
            if len(merged) >= limit:
                break
        if len(merged) >= limit:
            break
    return list(merged.keys())


def build_details(
    sentences: Sequence[str],
    sentiments: Sequence[str],
    topics: Sequence[Sequence[str]],
) -> List[Dict[str, object]]:
    """Build per-sentence detail entries."""

    detail_rows: List[Dict[str, object]] = []
    for sentence, sentiment, topic_list in zip(sentences, sentiments, topics):
        detail_rows.append(
            {
                "sentence": sentence,
                "sentiment": sentiment,
                "topics": list(topic_list),
            }
        )
    return detail_rows


def make_notebook_sentence(row: pd.Series) -> str:
    """Generate NotebookLM-friendly summary sentence for a review."""

    country = str(row.get("country", "")).upper()
    app_name = str(row.get("app_name", "user"))
    language = str(row.get("language", "en")).lower()
    label = str(row.get("sentiment_label", "neutral"))
    score = float(row.get("sentiment_score", 0.0))
    topics = row.get("topics") or []
    if isinstance(topics, str):
        topics = [t.strip() for t in topics.split(";") if t.strip()]
    topic_phrase = "; ".join(topics) if topics else "various aspects"

    language_name = language
    try:
        from langcodes import Language

        language_name = Language.get(language).display_name("en").lower()
    except Exception:
        pass

    details = row.get("details") or []
    if isinstance(details, str):
        try:
            details = json.loads(details)
        except json.JSONDecodeError:
            details = []

    positive_sentence = next(
        (d for d in details if str(d.get("sentiment")) == "positive"),
        None,
    )
    negative_sentence = next(
        (d for d in details if str(d.get("sentiment")) == "negative"),
        None,
    )
    neutral_sentence = next(
        (d for d in details if str(d.get("sentiment")) == "neutral"),
        None,
    )
    examples = []
    for entry, tag in (
        (positive_sentence, "POS"),
        (negative_sentence, "NEG"),
        (neutral_sentence, "NEU"),
    ):
        if entry:
            examples.append(f"‘{entry['sentence']}’ [{tag}]")
    examples_text = "; ".join(examples[:2]) if examples else ""

    score_text = f"{score:.2f}" if not math.isnan(score) else "0.00"
    summary = (
        f"In {country}, a {app_name.title()} user wrote in {language_name}. "
        f"The overall sentiment is {label} (score {score_text}). "
        f"It mainly discusses {topic_phrase}."
    )
    if examples_text:
        summary += f" Example sentences: {examples_text}."
    return summary


def write_csvs(
    structured_df: pd.DataFrame,
    notebook_df: pd.DataFrame,
    summary_df: pd.DataFrame,
    output_dir: Path,
) -> None:
    """Write pipeline outputs to CSV files and log destinations."""

    output_dir.mkdir(parents=True, exist_ok=True)
    reviews_path = output_dir / "reviews_sentiment_topics.csv"
    notebook_path = output_dir / "notebooklm_reviews.csv"
    summary_path = output_dir / "topic_summary.csv"

    structured_to_save = structured_df.copy()
    structured_to_save["topics"] = structured_to_save["topics"].apply(
        lambda vals: ";".join(vals) if isinstance(vals, (list, tuple)) else vals
    )
    structured_to_save["details"] = structured_to_save["details"].apply(json.dumps)

    structured_to_save.to_csv(reviews_path, index=False)
    notebook_df.to_csv(notebook_path, index=False)
    summary_df.to_csv(summary_path, index=False)

    LOGGER.info("Wrote structured reviews to %s", reviews_path.resolve())
    LOGGER.info("Wrote NotebookLM export to %s", notebook_path.resolve())
    LOGGER.info("Wrote topic summary to %s", summary_path.resolve())

__all__ = [
    "DEFAULT_COUNTRY_LANGUAGE_MAP",
    "detect_languages",
    "load_spacy_models",
    "split_sentences",
    "run_sentiment",
    "aggregate_sentiment",
    "extract_topics",
    "merge_topics",
    "build_details",
    "make_notebook_sentence",
    "write_csvs",
]
