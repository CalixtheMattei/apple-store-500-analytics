# ML Pipeline — Sentiment & Topics

This directory contains the reusable components for multilingual sentiment and topic
analysis used by the App Review Insights project.

## Running the notebook

1. Create a virtual environment and install dependencies:

   ```bash
   python -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   ```

2. Launch Jupyter Lab and open `notebooks/04_sentiment_topics_analysis.ipynb`:

   ```bash
   jupyter lab
   ```

3. Execute the notebook from top to bottom. It will:

   * Load processed reviews from `data/processed_reviews.csv`.
   * Detect languages (preferring the dataset column, falling back to country mapping
     and `langdetect`).
   * Split reviews into sentences, compute multilingual sentiment with CardiffNLP's
     RoBERTa model, and extract topics via KeyBERT + SentenceTransformer (with
     graceful fallbacks when offline).
   * Aggregate review-level insights and export three CSV files under
     `data/output/`:
     - `reviews_sentiment_topics.csv`
     - `notebooklm_reviews.csv`
     - `topic_summary.csv`

## Running headless

To reuse the logic in automation or Supabase pipelines, call the functions in
`ml/pipeline/sentiment_topics.py`. The module mirrors the notebook cells and exposes
pure helpers for each stage, e.g. `detect_languages`, `split_sentences`,
`run_sentiment`, and `write_csvs`.

### Example skeleton

```python
from pathlib import Path
import pandas as pd

from ml.pipeline import sentiment_topics as st

reviews = pd.read_csv("data/processed_reviews.csv")
reviews["detected_language"] = st.detect_languages(reviews)
models, lang_map = st.load_spacy_models(reviews["detected_language"].unique())
sentences = st.split_sentences(reviews, models, lang_map)
sentence_scores = st.run_sentiment(sentences)
# ... continue with aggregation & exports (see notebook for details)
```

## NotebookLM export

The NotebookLM export is generated via `make_notebook_sentence` for each review.
It produces natural-language summaries with embedded `[POS]`, `[NEG]`, and `[NEU]`
tags to highlight example sentences. The resulting CSV (`notebooklm_reviews.csv`)
can be uploaded directly to NotebookLM to seed conversational summaries.
