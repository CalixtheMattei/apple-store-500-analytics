"""Upload processed reviews to Supabase, skipping files with no new reviews."""

import json
from pathlib import Path

import pandas as pd

from scripts.utils_supabase import get_client

PROCESSED = Path("data/processed")
META = Path("data/metadata")


def load_latest_run_summary():
    if not META.exists():
        return [], None
    summaries = sorted(META.glob("run_clean_summary_*.json"))
    if not summaries:
        return [], None
    latest = max(summaries, key=lambda path: path.stat().st_mtime)
    try:
        data = json.loads(latest.read_text())
        return data, latest
    except json.JSONDecodeError:
        print(f"⚠️ Could not parse metadata file {latest.name}; continuing without it.")
        return [], latest


def upload_csv(path: Path, client):
    df = pd.read_csv(path)
    if df.empty:
        print(f"🔹 Skipped upload for {path.name} (empty file)")
        return

    records = df.to_dict(orient="records")
    try:
        client.table("clean_reviews").upsert(
            records, on_conflict="source,app_name,country,source_review_id"
        ).execute()
        print(f"⬆️ Uploaded {len(records)} rows from {path.name}")
    except Exception as exc:  # pragma: no cover - network failure path
        print(f"❌ Failed to upload {path.name}: {exc}")


def main():
    try:
        client = get_client()
    except EnvironmentError as exc:  # pragma: no cover - configuration path
        raise EnvironmentError("Missing Supabase credentials.") from exc

    metadata_entries, _ = load_latest_run_summary()
    status_by_file = {
        entry.get("processed_file"): entry for entry in metadata_entries if entry.get("processed_file")
    }

    no_new_entries = [
        entry for entry in metadata_entries if entry.get("status") == "no_new_reviews"
    ]

    if metadata_entries and all(entry.get("status") == "no_new_reviews" for entry in metadata_entries):
        print("🔹 All datasets marked as no new reviews; skipping Supabase upload.")
        for entry in no_new_entries:
            expected = entry.get("processed_file") or f"{entry.get('app', 'unknown')}_{entry.get('country', 'xx')}_clean.csv"
            print(f"🔹 Skipped upload for {expected} (no new reviews)")
        return

    for entry in no_new_entries:
        if not entry.get("processed_file_exists", True):
            expected = entry.get("processed_file") or f"{entry.get('app', 'unknown')}_{entry.get('country', 'xx')}_clean.csv"
            print(f"🔹 Skipped upload for {expected} (no new reviews)")

    csv_files = sorted(PROCESSED.glob("*_clean_*.csv"))
    if not csv_files:
        print(f"⚠️ No processed files found in {PROCESSED.resolve()}")
        return

    for csv_file in csv_files:
        entry = status_by_file.get(csv_file.name)
        if entry and entry.get("status") == "no_new_reviews":
            print(f"🔹 Skipped upload for {csv_file.name} (no new reviews)")
            continue
        upload_csv(csv_file, client)


if __name__ == "__main__":
    main()
