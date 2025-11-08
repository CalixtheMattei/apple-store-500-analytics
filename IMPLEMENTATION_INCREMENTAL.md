# Incremental Ingestion Implementation

## Updated Files
- `scripts/utils_supabase.py` — Added reusable Supabase client factory and batched helper for fetching existing `source_review_id` values by `(source, app, country)`.
- `scripts/02_process_reviews.py` — Integrated Supabase lookups, incremental filtering, richer metadata outputs, and run overview reporting.
- `scripts/03_upload_to_supabase.py` — Added metadata-aware upload orchestration with graceful skips for empty/no-new datasets and optional early exit.
- `.github/workflows/pipeline.yml` — Published metadata artifacts separately for observability.

## Behavioral Changes
- **Before:** Every processing run cleaned entire raw datasets and wrote processed CSVs regardless of Supabase state. Upload step attempted to upsert each CSV, even if rows already existed.
- **After:** Processing fetches existing IDs per cohort, filters them before cleaning, and records file-level metadata (including `status` and counts). When no new reviews remain, no processed CSV is written, yet metadata still signals the skip. The uploader reads that metadata, logs explicit "no new reviews" messages, skips empty uploads, and exits early when all statuses are `no_new_reviews`.

## Sample Logs
```
🔎 calmapp-us: comparing against 482 existing IDs in Supabase
🧮 Filtered 137 existing reviews prior to cleaning
✅ Saved calmapp_us_clean_2025-02-14_06-00-01.csv | 126/263 kept
🔹 All datasets marked as no new reviews; skipping Supabase upload.
🔹 Skipped upload for calmapp_us_clean_2025-02-14_06-00-01.csv (no new reviews)
```

## Future Optimizations
- Persist fetched ID sets to `data/cache/` for cross-script reuse within the same run, reducing duplicate Supabase requests.
- Add optional `since` filters (e.g., `review_date.gt(...)`) once run history tracks latest ingestion timestamps.
- Emit Supabase query timing metrics into metadata to support adaptive pagination sizes.
