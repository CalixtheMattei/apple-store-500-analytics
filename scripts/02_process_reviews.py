# scripts/02_process_reviews.py
"""
STEP 02 — Clean raw reviews
Reads raw JSON/CSV from data/raw, cleans text, detects language,
and outputs processed CSVs in data/processed.
"""

import pandas as pd, re, unicodedata, emoji, time, json
from datetime import datetime
from pathlib import Path
from langdetect import detect, DetectorFactory

DetectorFactory.seed = 0  # reproducible language detection

# === PATHS ===
BASE = Path("data")
RAW = BASE / "raw"
PROCESSED = BASE / "processed"
LOGS = BASE / "logs"
META = BASE / "metadata"

for p in (PROCESSED, LOGS, META):
    p.mkdir(parents=True, exist_ok=True)

run_time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

# === CONFIG ===
COUNTRY_LANG = {
    "fr": "fr", "us": "en", "gb": "en",
    "de": "de", "se": "sv", "es": "es",
    "it": "it", "ca": "en"
}

TEXT_COLUMN_CANDIDATES = [
    "content",
    "cleaned_content",
    "review_text",
    "review_body",
    "reviewbody",
    "text",
    "body",
    "comment",
    "description",
    "review",
]
TEXT_TOKEN_FALLBACK = {"content", "text", "body", "comment", "description"}
APP_FILE_PATTERN = re.compile(
    r"(?P<app>.+)_(?P<country>[a-z]{2})_(?P<date>\d{4}-\d{2}-\d{2})$",
    re.IGNORECASE,
)

# --- Helpers ---
def clean_text(text: str) -> str:
    if not isinstance(text, str):
        return ""
    text = text.lower().strip()
    text = emoji.replace_emoji(text, "")
    text = re.sub(r"http\\S+|www\\S+", "", text)
    text = re.sub(r"[^a-zA-Z0-9À-ÿ\\s']", " ", text)
    text = unicodedata.normalize("NFKC", text)
    text = re.sub(r"\\s+", " ", text)
    return text.strip()

def extract_app_country(filename: str):
    stem = Path(filename).stem
    m = APP_FILE_PATTERN.match(stem)
    if m:
        return m.group("app"), m.group("country").lower()
    return ("unknown", "xx")


def find_text_column(columns):
    normalized = {c.lower(): c for c in columns}
    for candidate in TEXT_COLUMN_CANDIDATES:
        match = normalized.get(candidate)
        if match:
            return match

    for col in columns:
        tokens = re.split(r"[_\\s]", col.lower())
        if any(token in TEXT_TOKEN_FALLBACK for token in tokens if token):
            return col
    return None

def detect_language_safe(text: str):
    try:
        return detect(text)
    except Exception:
        return "unknown"

# === MAIN CLEANING LOOP ===
summary = []
start = time.time()

files = list(RAW.glob("*.json")) + list(RAW.glob("*.csv"))
print(f"📦 Found {len(files)} raw files in {RAW.resolve()}")

for f in files:
    app, country = extract_app_country(f.name)
    print(f"🧹 Cleaning {f.name} ({app.upper()} - {country.upper()})")

    # Load file
    try:
        if f.suffix == ".json":
            with open(f, "r", encoding="utf-8") as fp:
                data = json.load(fp)
            df = pd.json_normalize(data)
        else:
            df = pd.read_csv(f)
    except Exception as e:
        print(f"⚠️ Could not read {f.name}: {e}")
        continue

    n_before = len(df)
    text_col = find_text_column(df.columns)
    if not text_col:
        print(f"⚠️ No text/content column found, skipping {f.name}")
        continue

    # Preserve original content and build cleaned text variant
    df["content"] = df[text_col].astype(str)
    df["cleaned_content"] = df["content"].apply(clean_text)

    df = (
        df[df["cleaned_content"].str.len() > 10]
        .drop_duplicates(subset=["cleaned_content"])
        .dropna(subset=["cleaned_content"])
    )
    if df.empty:
        print("   ⚠ No reviews left after text cleaning; skipping file.")
        summary.append({
            "file": f.name,
            "app": app,
            "country": country,
            "n_before": n_before,
            "n_after": 0,
            "pct_kept": 0.0
        })
        continue

    # Language filter
    expected_lang = COUNTRY_LANG.get(country, "en")
    print(f"🌐 Detecting language (expecting {expected_lang})...")
    df["language"] = df["cleaned_content"].apply(lambda x: detect_language_safe(x[:200]))
    before_lang = len(df)
    df = df[df["language"] == expected_lang]
    after_lang = len(df)
    print(f"   → {after_lang}/{before_lang} kept ({round(100*after_lang/before_lang,2)}%)")

    # Normalize dates
    date_col = next((c for c in df.columns if "date" in c.lower()), None)
    if date_col:
        df[date_col] = pd.to_datetime(df[date_col], errors="coerce").dt.date
        if date_col != "review_date":
            df["review_date"] = df[date_col]
    else:
        df["review_date"] = pd.NaT

    # Enrichment
    df["app_name"] = app
    df["country"] = country

    # Align with Supabase schema expectations
    required_columns = [
        "app_name",
        "country",
        "source",
        "source_review_id",
        "rating",
        "title",
        "content",
        "cleaned_content",
        "language",
        "review_date",
    ]
    for col in required_columns:
        if col not in df.columns:
            df[col] = pd.NA
    df = df[required_columns]

    # Save processed file
    out_path = PROCESSED / f"{app}_{country}_clean_{run_time}.csv"
    df.to_csv(out_path, index=False)
    print(f"✅ Saved {out_path.name} | {len(df)}/{n_before} kept")

    summary.append({
        "file": f.name,
        "app": app,
        "country": country,
        "n_before": n_before,
        "n_after": len(df),
        "pct_kept": round(100 * len(df) / n_before, 2) if n_before else 0.0
    })

# === SUMMARY ===
summary_df = pd.DataFrame(summary)
print("\n📊 Cleaning summary:")
print(summary_df.to_string(index=False))
summary_path = META / f"run_clean_summary_{run_time}.json"
summary_path.write_text(summary_df.to_json(orient="records", indent=2))
print(f"🗒️ Saved run summary → {summary_path}")
print(f"⏱️ Total runtime: {time.time() - start:.1f}s")
