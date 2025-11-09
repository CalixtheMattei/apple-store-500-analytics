"""Upload processed reviews to Supabase, skipping files with no new reviews."""

from __future__ import annotations

import json
import math
from datetime import date, datetime
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd
from pandas.errors import EmptyDataError

from utils_supabase import get_client

ROOT = Path(__file__).resolve().parents[1]
PROCESSED = ROOT / "data" / "processed"
META = ROOT / "data" / "metadata"
BATCH_SIZE = 300


def load_latest_run_summary() -> Tuple[List[Dict], Path | None]:
    """Return the latest run summary entries and the file they came from."""
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
        print(f"[WARN] Could not parse metadata file {latest.name}; continuing without it.")
        return [], latest


def _describe_entry(entry: Dict) -> str:
    return entry.get("processed_file") or f"{entry.get('app', 'unknown')}_{entry.get('country', 'xx')}_clean.csv"


def _normalize_record(record: Dict) -> Dict:
    normalized = {}
    for key, value in record.items():
        normalized[key] = _normalize_value(key, value)
    return normalized


def _normalize_value(column: str, value):
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    if pd.isna(value):
        return None

    if column == "rating":
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    if column == "review_date":
        coerced = _to_date_string(value)
        return coerced or value

    return value


def _to_date_string(value):
    if isinstance(value, pd.Timestamp):
        value = value.to_pydatetime()
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return None


def upload_csv(path: Path, client):
    try:
        df = pd.read_csv(path)
    except EmptyDataError:
        print(f"[SKIP] {path.name} appears to be empty; skipping.")
        return
    except OSError as exc:
        print(f"[WARN] Could not read {path.name}: {exc}")
        return

    if df.empty:
        print(f"[SKIP] Skipped upload for {path.name} (empty file).")
        return

    records = [_normalize_record(record) for record in df.to_dict(orient="records")]
    total_uploaded = 0

    for offset in range(0, len(records), BATCH_SIZE):
        chunk = records[offset : offset + BATCH_SIZE]
        try:
            client.table("clean_reviews").upsert(
                chunk, on_conflict="source,app_name,country,source_review_id"
            ).execute()
            total_uploaded += len(chunk)
        except Exception as exc:  # pragma: no cover - network failure path
            print(f"[ERROR] Failed to upload batch {offset // BATCH_SIZE + 1} from {path.name}: {exc}")
            return

    print(f"[OK] Uploaded {total_uploaded} rows from {path.name}.")


def determine_target_files(metadata_entries: List[Dict]) -> Tuple[List[Path], Dict[str, Dict], List[str]]:
    status_by_file = {
        entry.get("processed_file"): entry for entry in metadata_entries if entry.get("processed_file")
    }

    target_files: List[Path] = []
    missing_files: List[str] = []

    if metadata_entries:
        for entry in metadata_entries:
            processed_name = entry.get("processed_file")
            if not processed_name:
                continue
            path = PROCESSED / processed_name
            if path.exists():
                target_files.append(path)
            else:
                missing_files.append(processed_name)
    else:
        target_files = sorted(PROCESSED.glob("*_clean_*.csv"))

    unique_files = []
    seen = set()
    for path in target_files:
        if path.name not in seen:
            unique_files.append(path)
            seen.add(path.name)

    return unique_files, status_by_file, missing_files


def main():
    try:
        client = get_client()
    except EnvironmentError as exc:  # pragma: no cover - configuration path
        raise EnvironmentError("Missing Supabase credentials.") from exc

    metadata_entries, meta_path = load_latest_run_summary()
    target_files, status_by_file, missing_files = determine_target_files(metadata_entries)

    if meta_path:
        print(f"[INFO] Loaded {len(metadata_entries)} metadata entries from {meta_path.name}.")

    for missing in missing_files:
        print(f"[WARN] Metadata references {missing} but the file was not found.")

    if not target_files:
        print(f"[WARN] No processed files found in {PROCESSED.resolve()}.")
        return

    skip_names = {
        name
        for name, entry in status_by_file.items()
        if entry.get("status") == "no_new_reviews"
    }

    for entry in metadata_entries:
        if entry.get("status") == "no_new_reviews" and not entry.get("processed_file_exists", True):
            print(f"[SKIP] {_describe_entry(entry)} marked as no new reviews (no file generated).")

    for csv_file in target_files:
        entry = status_by_file.get(csv_file.name)
        if csv_file.name in skip_names:
            print(f"[SKIP] {csv_file.name} marked as no new reviews in metadata.")
            continue
        upload_csv(csv_file, client)


if __name__ == "__main__":
    main()
