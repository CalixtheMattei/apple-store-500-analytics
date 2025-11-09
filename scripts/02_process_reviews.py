"""STEP 02 — Clean raw reviews with incremental Supabase filtering."""

import json
import re
import time
import unicodedata
from datetime import datetime
from pathlib import Path
from typing import Dict, Tuple

import emoji
import pandas as pd
from langdetect import DetectorFactory, detect

from utils_supabase import get_existing_ids

DetectorFactory.seed = 0  # reproducible language detection

# === PATHS ===
BASE = Path("data")
RAW = BASE / "raw"
PROCESSED = BASE / "processed"
LOGS = BASE / "logs"
META = BASE / "metadata"

for path in (PROCESSED, LOGS, META):
    path.mkdir(parents=True, exist_ok=True)

run_time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

# === CONFIG ===
COUNTRY_LANG = {
    "fr": "fr",
    "us": "en",
    "gb": "en",
    "de": "de",
    "se": "sv",
    "es": "es",
    "it": "it",
    "ca": "en",
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

# === INCREMENTAL CACHE ===
_existing_id_cache: Dict[Tuple[str, str, str], set] = {}
incremental_enabled = True


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


def extract_app_country(filename: str) -> Tuple[str, str]:
    stem = Path(filename).stem
    match = APP_FILE_PATTERN.match(stem)
    if match:
        return match.group("app"), match.group("country").lower()
    return "unknown", "xx"


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


def detect_language_safe(text: str) -> str:
    try:
        return detect(text)
    except Exception:
        return "unknown"


def infer_source(df: pd.DataFrame) -> str:
    if "source" in df.columns:
        first_valid = df["source"].dropna()
        if not first_valid.empty:
            value = str(first_valid.iloc[0]).strip()
            if value:
                return value
    return "app_store"


def fetch_existing_ids(source: str, app: str, country: str):
    """Fetch and cache existing source_review_id values for this cohort."""

    global incremental_enabled
    cache_key = (source, app, country)
    if not incremental_enabled:
        return set(), False, False

    if cache_key in _existing_id_cache:
        return _existing_id_cache[cache_key], True, True

    try:
        ids = get_existing_ids(source, app, country)
        print(
            f"🔎 {app}-{country}: comparing against {len(ids)} existing IDs in Supabase"
        )
        _existing_id_cache[cache_key] = ids
        return ids, False, True
    except Exception as exc:  # pragma: no cover - network failure path
        print(f"⚠️ Incremental mode disabled for {app}-{country}: {exc}")
        incremental_enabled = False
        return set(), False, False


# === MAIN CLEANING LOOP ===
summary = []
start = time.time()

files = sorted(list(RAW.glob("*.json")) + list(RAW.glob("*.csv")))
print(f"📦 Found {len(files)} raw files in {RAW.resolve()}")

for raw_path in files:
    app, country = extract_app_country(raw_path.name)
    processed_filename = f"{app}_{country}_clean_{run_time}.csv"
    print(f"🧹 Cleaning {raw_path.name} ({app.upper()} - {country.upper()})")

    try:
        if raw_path.suffix == ".json":
            with open(raw_path, "r", encoding="utf-8") as fp:
                data = json.load(fp)
            df = pd.json_normalize(data)
        else:
            df = pd.read_csv(raw_path)
    except Exception as exc:  # pragma: no cover - defensive logging
        print(f"⚠️ Could not read {raw_path.name}: {exc}")
        continue

    n_raw = len(df)
    text_col = find_text_column(df.columns)
    if not text_col:
        print(f"⚠️ No text/content column found, skipping {raw_path.name}")
        continue

    source = infer_source(df)
    existing_ids, cache_hit, incremental_active = fetch_existing_ids(source, app, country)
    existing_checked = len(existing_ids)
    skipped_existing = 0

    if incremental_active and existing_ids and "source_review_id" in df.columns:
        before_filter = len(df)
        df = df[~df["source_review_id"].astype(str).isin(existing_ids)]
        skipped_existing = before_filter - len(df)
        if skipped_existing:
            print(f"🧮 Filtered {skipped_existing} existing reviews prior to cleaning")
    elif incremental_active and "source_review_id" not in df.columns:
        print("⚠️ source_review_id column missing; incremental filtering skipped.")

    if df.empty:
        status = "no_new_reviews"
        print(f"🔹 No new reviews for {app}-{country}; skipping cleaning steps.")
        file_metadata = {
            "file": raw_path.name,
            "app": app,
            "country": country,
            "source": source,
            "run_time": run_time,
            "processed_file": processed_filename,
            "processed_file_exists": False,
            "n_raw": n_raw,
            "existing_reviews_checked": existing_checked,
            "skipped_existing": skipped_existing,
            "new_reviews_cleaned": 0,
            "pct_kept": 0.0,
            "status": status,
            "incremental_active": incremental_active,
            "incremental_cache_hit": cache_hit,
        }
        summary.append(file_metadata)
        (META / f"{app}_{country}_metadata_{run_time}.json").write_text(
            json.dumps(file_metadata, indent=2)
        )
        continue

    df["content"] = df[text_col].astype(str)
    df["cleaned_content"] = df["content"].apply(clean_text)

    df = (
        df[df["cleaned_content"].str.len() > 10]
        .drop_duplicates(subset=["cleaned_content"])
        .dropna(subset=["cleaned_content"])
    )
    if df.empty:
        print("   ⚠ No reviews left after text cleaning; skipping file.")
        status = "no_new_reviews"
        file_metadata = {
            "file": raw_path.name,
            "app": app,
            "country": country,
            "source": source,
            "run_time": run_time,
            "processed_file": processed_filename,
            "processed_file_exists": False,
            "n_raw": n_raw,
            "existing_reviews_checked": existing_checked,
            "skipped_existing": skipped_existing,
            "new_reviews_cleaned": 0,
            "pct_kept": 0.0,
            "status": status,
            "incremental_active": incremental_active,
            "incremental_cache_hit": cache_hit,
        }
        summary.append(file_metadata)
        (META / f"{app}_{country}_metadata_{run_time}.json").write_text(
            json.dumps(file_metadata, indent=2)
        )
        continue

    expected_lang = COUNTRY_LANG.get(country, "en")
    print(f"🌐 Detecting language (expecting {expected_lang})...")
    df["language"] = df["cleaned_content"].apply(lambda x: detect_language_safe(x[:200]))
    before_lang = len(df)
    df = df[df["language"] == expected_lang]
    after_lang = len(df)
    pct_lang = round(100 * after_lang / before_lang, 2) if before_lang else 0.0
    print(f"   → {after_lang}/{before_lang} kept ({pct_lang}%)")

    date_col = next((c for c in df.columns if "date" in c.lower()), None)
    if date_col:
        df[date_col] = pd.to_datetime(df[date_col], errors="coerce").dt.date
        if date_col != "review_date":
            df["review_date"] = df[date_col]
    else:
        df["review_date"] = pd.NaT

    df["app_name"] = app
    df["country"] = country
    df["source"] = source

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
    for column in required_columns:
        if column not in df.columns:
            df[column] = pd.NA
    df = df[required_columns]

    out_path = PROCESSED / processed_filename
    df.to_csv(out_path, index=False)
    print(f"✅ Saved {out_path.name} | {len(df)}/{n_raw} kept")

    status = "new_dataset" if skipped_existing == 0 else "partial_update"
    if len(df) == 0:
        status = "no_new_reviews"

    pct_kept = round(100 * len(df) / n_raw, 2) if n_raw else 0.0
    file_metadata = {
        "file": raw_path.name,
        "app": app,
        "country": country,
        "source": source,
        "run_time": run_time,
        "processed_file": processed_filename,
        "processed_file_exists": True,
        "n_raw": n_raw,
        "existing_reviews_checked": existing_checked,
        "skipped_existing": skipped_existing,
        "new_reviews_cleaned": len(df),
        "pct_kept": pct_kept,
        "status": status,
        "incremental_active": incremental_active,
        "incremental_cache_hit": cache_hit,
    }
    summary.append(file_metadata)
    (META / f"{app}_{country}_metadata_{run_time}.json").write_text(
        json.dumps(file_metadata, indent=2, default=str)
    )

# === SUMMARY ===
summary_df = pd.DataFrame(summary)
print("\n📊 Cleaning summary:")
if not summary_df.empty:
    print(
        summary_df[
            ["app", "country", "status", "n_raw", "new_reviews_cleaned", "skipped_existing"]
        ].to_string(index=False)
    )
else:
    print("(no files processed)")

summary_path = META / f"run_clean_summary_{run_time}.json"
summary_path.write_text(summary_df.to_json(orient="records", indent=2))
print(f"🗒️ Saved run summary → {summary_path}")

run_overview = {
    "run_time": run_time,
    "files_found": len(files),
    "files_processed": len(summary),
    "total_raw_reviews": int(summary_df["n_raw"].sum()) if not summary_df.empty else 0,
    "total_new_reviews": int(summary_df["new_reviews_cleaned"].sum()) if not summary_df.empty else 0,
    "total_skipped_existing": int(summary_df["skipped_existing"].sum()) if not summary_df.empty else 0,
    "total_existing_checked": int(summary_df["existing_reviews_checked"].sum()) if not summary_df.empty else 0,
    "all_no_new_reviews": bool(
        not summary_df.empty and (summary_df["status"] == "no_new_reviews").all()
    ),
    "incremental_mode_enabled": incremental_enabled,
    "details": summary,
}
overview_path = META / f"run_incremental_overview_{run_time}.json"
overview_path.write_text(json.dumps(run_overview, indent=2))
print(f"🧾 Saved incremental overview → {overview_path}")

elapsed = time.time() - start
print(f"⏱️ Total runtime: {elapsed:.1f}s")
