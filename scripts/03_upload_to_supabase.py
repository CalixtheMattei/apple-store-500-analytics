import os
import pandas as pd
from supabase import create_client
from pathlib import Path

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def upload_csv(path: Path):
    df = pd.read_csv(path)
    if df.empty:
        print(f"⚠️ Skipped empty file {path}")
        return
    records = df.to_dict(orient="records")
    supabase.table("clean_reviews").upsert(
        records, on_conflict="source,app_name,country,source_review_id"
    ).execute()
    print(f"⬆️ Uploaded {len(records)} rows from {path.name}")

def main():
    processed = Path("data/processed")
    for csv_file in processed.glob("*_clean.csv"):
        upload_csv(csv_file)

if __name__ == "__main__":
    main()
