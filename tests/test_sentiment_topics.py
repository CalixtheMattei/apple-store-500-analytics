import json
from pathlib import Path
import sys

import pandas as pd
import pytest

pytest.importorskip("spacy")

sys.path.append(str(Path(__file__).resolve().parents[1]))

from ml.pipeline import sentiment_topics as st


@pytest.fixture
def sample_reviews():
    return pd.DataFrame(
        [
            {
                "id": 1,
                "app_name": "yubo",
                "country": "it",
                "language": "it",
                "cleaned_content": "Adoro l'app ma a volte si blocca",
            },
            {
                "id": 2,
                "app_name": "yubo",
                "country": "ca",
                "cleaned_content": "Great idea though the chat keeps crashing",
            },
        ]
    )


def test_detect_languages_prefers_column(sample_reviews):
    langs = st.detect_languages(sample_reviews)
    assert list(langs) == ["it", "en"]


def test_aggregate_sentiment_mixed_label():
    label, score = st.aggregate_sentiment(["positive", "negative", "positive", "negative"])
    assert label == "mixed"
    assert pytest.approx(score, abs=1e-6) == 0.0


def test_merge_topics_deduplicates_and_limits():
    topics = st.merge_topics([["ui", "bugs"], ["bugs", "chat issues"], ["latency"]], limit=2)
    assert topics == ["ui", "bugs"]


def test_make_notebook_sentence_includes_tags():
    row = pd.Series(
        {
            "country": "it",
            "app_name": "yubo",
            "language": "it",
            "sentiment_label": "mixed",
            "sentiment_score": -0.1,
            "topics": ["ui design", "bugs"],
            "details": json.dumps(
                [
                    {"sentence": "Mi piace l'interfaccia", "sentiment": "positive", "topics": ["ui design"]},
                    {"sentence": "La chat si blocca", "sentiment": "negative", "topics": ["bugs"]},
                ]
            ),
        }
    )
    sentence = st.make_notebook_sentence(row)
    assert "[POS]" in sentence and "[NEG]" in sentence


def test_build_details_keeps_topics():
    sentences = ["I love it", "It crashes"]
    sentiments = ["positive", "negative"]
    topics = [["design"], ["bugs"]]
    details = st.build_details(sentences, sentiments, topics)
    assert details[0]["topics"] == ["design"]
    assert details[1]["sentiment"] == "negative"
