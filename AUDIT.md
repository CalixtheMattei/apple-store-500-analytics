# 📊 Pipeline Audit Report (Codex)

## ✅ Working as expected
- `config/apps.json` structure (apps, countries, source, scrape delay) matches how `scripts/01_scrape.py` loads configuration and iterates scraping targets, ensuring consistent runtime parameters.【F:config/apps.json†L1-L13】【F:scripts/01_scrape.py†L14-L107】
- The scraper captures `source_review_id`, rating metadata, review text, and source identifiers that align with columns defined in the Supabase `clean_reviews` schema, and persists them under `data/raw/` for downstream steps.【F:scripts/01_scrape.py†L57-L95】【F:sql/001_create_tables.sql†L4-L15】
- The Supabase DDL creates a unique index on `(source, app_name, country, source_review_id)`, which matches the `on_conflict` clause used during upsert in `03_upload_to_supabase.py`, preventing duplicate rows on re-scrapes.【F:sql/001_create_tables.sql†L17-L19】【F:scripts/03_upload_to_supabase.py†L16-L25】
- The GitHub Actions workflow installs dependencies and runs the three pipeline steps (scrape → process → upload) in the intended order while injecting Supabase secrets for the upload stage.【F:.github/workflows/pipeline.yml†L17-L36】

## ⚠️ Issues / Inconsistencies
- Processed CSV filenames include a timestamp suffix (e.g., `*_clean_2025-11-08_20-06-53.csv`), but the uploader only searches for files matching `*_clean.csv`, so no processed data will ever be uploaded.【F:scripts/02_process_reviews.py†L151-L158】【F:scripts/03_upload_to_supabase.py†L22-L25】
- The processed dataset introduces a `lang` column and retains helper fields such as `year_month` and `review_length`, none of which exist in the `clean_reviews` table; attempting to upsert these records into Supabase will raise column mismatch errors.【F:scripts/02_process_reviews.py†L138-L154】【F:sql/001_create_tables.sql†L4-L13】
- The cleaning step overwrites the `content` column with normalized text but never populates the `cleaned_content` or `language` fields expected by the Supabase schema, causing the upload to omit required analytics columns and fail schema validation.【F:scripts/02_process_reviews.py†L120-L153】【F:sql/001_create_tables.sql†L10-L12】
- The uploader relies on raw environment variable access at import time; if the GitHub Action secrets are misconfigured or missing, the module import will crash before `main()` executes, preventing graceful error handling or logging.【F:scripts/03_upload_to_supabase.py†L6-L10】
- Requirements list includes `dotenv`, but no script loads `.env` files—locally running the uploader without exporting variables will fail. Either document the expectation or add `python-dotenv` integration for parity with CI secrets.【F:requirements.txt†L1-L9】【F:scripts/03_upload_to_supabase.py†L6-L10】

## 🧩 Suggestions for improvement
- Align processed CSV naming with the uploader by either adjusting the glob to `*_clean_*.csv` or removing the timestamp suffix so uploads run automatically in CI.【F:scripts/02_process_reviews.py†L151-L158】【F:scripts/03_upload_to_supabase.py†L22-L25】
- Map processed columns to the Supabase schema before upsert: preserve raw text in `content`, add a separate `cleaned_content`, and rename `lang` → `language` while dropping auxiliary fields (`year_month`, `review_length`) prior to upload.【F:scripts/02_process_reviews.py†L120-L154】【F:sql/001_create_tables.sql†L10-L12】
- Add defensive handling around Supabase client creation (e.g., using `os.getenv` with validation inside `main()` and raising a clear message) to improve resilience when secrets are missing or rotated.【F:scripts/03_upload_to_supabase.py†L6-L27】
- Document or implement local secret loading by using `python-dotenv` (instead of the unused `dotenv` package) so contributors can mirror CI behavior during manual runs.【F:requirements.txt†L6-L9】【F:scripts/03_upload_to_supabase.py†L6-L10】

## 💡 Optional enhancements
- Extend the uploader to chunk large upserts and log Supabase responses for observability, making retries easier to diagnose during CI runs.【F:scripts/03_upload_to_supabase.py†L11-L20】
- Capture both raw and cleaned text in the processed output (e.g., `raw_content`, `cleaned_content`) for richer downstream analytics and to support schema evolution without re-scraping.【F:scripts/02_process_reviews.py†L120-L154】
- Publish processed summaries (`run_clean_summary_*.json`) as GitHub Action artifacts or Supabase tables for monitoring ingestion health over time.【F:scripts/02_process_reviews.py†L169-L176】【F:.github/workflows/pipeline.yml†L32-L36】

---

*Generated automatically by Codex on 2025-11-08*
