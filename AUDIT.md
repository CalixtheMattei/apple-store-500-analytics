# 📊 Pipeline Audit Report (Codex)

## ✅ Working as expected
- `scripts/01_scrape.py` creates the `data/raw` directory before saving outputs and persists per-run summaries, so downstream steps have a predictable input location.【F:scripts/01_scrape.py†L10-L107】
- `scripts/02_process_reviews.py` provisions the `data/processed`, `data/logs`, and `data/metadata` folders, performs text normalization, language detection, and adds enrichment metrics to each cleaned dataset.【F:scripts/02_process_reviews.py†L16-L158】
- The Supabase upload step reads from processed CSVs, uses service-role credentials, and upserts into `clean_reviews` with a conflict target that matches the unique index defined in the schema.【F:scripts/03_upload_to_supabase.py†L6-L25】【F:sql/001_create_tables.sql†L2-L19】
- GitHub Actions installs dependencies, runs the scrape → process → upload scripts sequentially, and wires Supabase secrets into the uploader step, mirroring the intended local workflow.【F:.github/workflows/pipeline.yml†L8-L36】

## ⚠️ Issues / Inconsistencies
- The scraper hardcodes `config/apps.json`, but the repository does not ship a `config/` directory, so the very first pipeline step will raise `FileNotFoundError` when run from a fresh clone.【F:scripts/01_scrape.py†L10-L83】
- Processed filenames are emitted as `{app}_{country}_clean_{timestamp}.csv`, yet the uploader only scans for `*_clean.csv`, so no processed file will be discovered or upserted.【F:scripts/02_process_reviews.py†L155-L158】【F:scripts/03_upload_to_supabase.py†L22-L25】
- The processor writes columns named `lang`, `year_month`, and `review_length`, but these do not exist on `clean_reviews`; sending them in an upsert will produce a PostgREST error. Likewise, the processor never fills the `language` or `cleaned_content` fields the table defines (it overwrites `content` in place).【F:scripts/02_process_reviews.py†L120-L154】【F:sql/001_create_tables.sql†L2-L13】
- Because language is stored under `lang` instead of `language`, the Supabase table will keep that column `NULL`, breaking downstream views or filters that expect populated values.【F:scripts/02_process_reviews.py†L138-L141】【F:sql/001_create_tables.sql†L2-L13】
- The SQL view aggregates on `review_date`, but the processor only coerces timestamps when a column containing "date" exists; if raw files omit or rename this field, the view will return NULL days without safeguards or logging.【F:scripts/02_process_reviews.py†L144-L149】【F:sql/002_views_and_indexes.sql†L1-L10】

## 🧩 Suggestions for improvement
- Commit a sample `config/apps.json` template (or guard `load_config()` with clearer error messaging) so first runs succeed without manual file creation.【F:scripts/01_scrape.py†L10-L83】
- Align processed filenames and uploader globbing—either change the processor to emit `*_clean.csv` or expand the uploader pattern (e.g., `*_clean_*.csv`).【F:scripts/02_process_reviews.py†L155-L158】【F:scripts/03_upload_to_supabase.py†L22-L25】
- Normalize column names before upload: rename `lang` → `language`, persist the pre-clean text as `content`, store the cleaned string in `cleaned_content`, and drop helper columns (`year_month`, `review_length`) or create matching Supabase fields/views.【F:scripts/02_process_reviews.py†L120-L154】【F:sql/001_create_tables.sql†L2-L13】
- Add schema-aware validation in the uploader (e.g., check for required keys, cast dates to ISO strings) and wrap the upsert call with error handling to surface PostgREST responses early.【F:scripts/03_upload_to_supabase.py†L11-L20】
- Extend the view to guard against NULL `review_date` rows or have the processor default missing review dates to the scrape day to keep aggregates stable.【F:scripts/02_process_reviews.py†L144-L149】【F:sql/002_views_and_indexes.sql†L1-L10】

## 💡 Optional enhancements
- Batch large uploads (e.g., chunk records) and add simple retry logic with exponential backoff for transient Supabase errors.【F:scripts/03_upload_to_supabase.py†L11-L20】
- Emit structured logs (JSON or CSV) from each stage and include them in the GitHub Action artifact to aid post-run diagnostics.【F:scripts/02_process_reviews.py†L169-L175】【F:.github/workflows/pipeline.yml†L32-L36】
- Store run metadata in Supabase (e.g., `pipeline_runs` table) to track scrape volumes and processing stats alongside review data.【F:scripts/01_scrape.py†L98-L107】【F:scripts/02_process_reviews.py†L169-L175】

---

*Generated automatically by Codex on 2024-07-08*
