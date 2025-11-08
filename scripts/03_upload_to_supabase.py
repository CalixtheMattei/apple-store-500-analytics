"""Upload processed reviews to Supabase."""

import os
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()


def upload_csv(path: Path, client):
    df = pd.read_csv(path)
    if df.empty:
        print(f"⚠️ Skipped empty file {path}")
        return

    records = df.to_dict(orient="records")

    try:
        client.table("clean_reviews").upsert(
            records, on_conflict="source,app_name,country,source_review_id"
        ).execute()
        print(f"⬆️ Uploaded {len(records)} rows from {path.name}")
    except Exception as exc:
        print(f"❌ Failed to upload {path.name}: {exc}")


def main():
    processed = Path("data/processed")

    supabase_url = os.getenv("SUPABASE_URL")
    supabase_key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

    if not supabase_url or not supabase_key:
        raise EnvironmentError("Missing Supabase credentials.")

    client = create_client(supabase_url, supabase_key)

    csv_files = sorted(processed.glob("*_clean_*.csv"))
    if not csv_files:
        print(f"⚠️ No processed files found in {processed.resolve()}")
        return

    for csv_file in csv_files:
        upload_csv(csv_file, client)


if __name__ == "__main__":
    main()
