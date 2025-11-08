# ⚙️ Incremental Cleaning Audit Report

## ✅ Current Behavior
- **Scraping (`scripts/01_scrape.py`)**
  - Loads the app/country scope from `config/apps.json` and iterates every combination on each run.【F:scripts/01_scrape.py†L10-L98】
  - Saves one CSV snapshot per app-country with all fetched reviews (~500) under `data/raw/` and records a run summary JSON.【F:scripts/01_scrape.py†L92-L107】
- **Processing (`scripts/02_process_reviews.py`)**
  - Scans every raw file, cleans text, enforces language, normalizes schema, and writes a processed CSV regardless of Supabase state.【F:scripts/02_process_reviews.py†L95-L193】
  - Produces a run summary JSON but does not track whether reviews already exist upstream.【F:scripts/02_process_reviews.py†L195-L201】
- **Uploading (`scripts/03_upload_to_supabase.py`)**
  - Reads all processed CSVs and upserts them into the `clean_reviews` table using the composite unique key `(source, app_name, country, source_review_id)`.【F:scripts/03_upload_to_supabase.py†L13-L47】【F:sql/001_create_tables.sql†L1-L15】
  - No pre-check is done to avoid reprocessing or uploading existing rows.

## ⚠️ Limitations
- **Redundant cleaning**: Every execution re-cleans the entire raw snapshot even if all `source_review_id`s already exist in Supabase, wasting CPU and time when datasets stagnate.
- **Upload churn**: Upserts re-send rows Supabase already stores, driving unnecessary bandwidth and slower API calls.
- **Lack of delta awareness**: Current summaries cannot distinguish between “no new reviews” vs “new rows ingested,” making observability harder.

## 🧩 Proposed Incremental Design
### 1. Supabase lookup strategy
- Introduce a helper that retrieves existing `source_review_id`s filtered by `source`, `app_name`, and `country`, using paginated `.range()` calls of 1,000 rows (Supabase REST limit) until no more rows are returned.
- Fetch IDs **per app-country** immediately before processing that raw file to bound memory to the size of that cohort (hundreds vs entire table) while keeping network calls manageable (≤1 per file when results cached locally per run).
- Select only `source_review_id` to minimize payloads; rely on the current unique index for efficient lookups.【F:sql/001_create_tables.sql†L1-L15】
- Cache run-local results in a dictionary keyed by `(source, app, country)` so that multiple raw files for the same combination reuse the same ID list during the same execution.

### 2. Filtering & cleaning changes (`02_process_reviews.py`)
- Before language filtering, request existing IDs for the file’s `(source, app, country)` combination. When Supabase credentials are absent (e.g., local offline run), log a warning and fall back to current behavior.
- After building the DataFrame but **before** expensive language detection, drop any rows whose `source_review_id` appears in the fetched ID set. Track counts for `n_existing`, `n_new`, and `n_processed`.
- If the resulting DataFrame is empty, skip downstream steps but write an empty processed file (or just metadata) and generate a summary entry with status `"no_new_reviews"` to drive consistent automation. Optionally emit a lightweight CSV containing just headers to keep uploader compatible.

### 3. Upload step adjustments (`03_upload_to_supabase.py`)
- Enhance logging to indicate when a processed file contains zero rows (e.g., “no new reviews for tinder_us”). Skip calling `upsert` when the CSV is empty.
- Optionally read metadata generated in the previous step to decide whether to skip file iteration entirely when `status == "no_new_reviews"` (future optimization).

### 4. Observability & metadata
- Extend the cleaning summary JSON to include:
  - `existing_reviews_checked`: number of Supabase rows compared for this combination.
  - `new_reviews_cleaned`: number of rows kept for upload after deduping.
  - `skipped_reviews`: count of raw rows dropped because they already existed upstream.
  - `status`: enum of `no_new_reviews`, `partial_update`, or `new_dataset` based on counts.
- Emit a run-level metadata file (e.g., `run_incremental_overview_{timestamp}.json`) aggregating totals and listing combinations with no new data so automation can alert or skip later steps.

### 5. Optimization considerations
- Maintain per-run in-memory caching of ID sets; if a combination produces large result sets (>10k IDs) consider persisting to a temporary file within `data/cache/` for reuse by subsequent scripts in the same run.
- Support optional `since` filtering by passing `review_date.gt(last_seen_date)` when metadata from previous runs records the latest ingestion timestamp. Default to full history lookup until such metadata exists.
- If Supabase latency becomes material, explore batching app-country lookups by country or storing the most recent IDs in Supabase storage for quick download.

## 🧱 Required Code Changes
- **`scripts/02_process_reviews.py`**
  1. Add Supabase client bootstrap (shared helper module or inline) that reads credentials lazily and exposes `get_existing_review_ids(source, app, country, client, batch_size=1000)` using paginated selects.
  2. Insert a preprocessing step that loads existing IDs, filters `df` using `df["source_review_id"].isin(existing_ids)` before language detection, and records counts (`n_existing`, `n_new`).
  3. Update summary dictionary to include new metrics and a `status` field derived from counts.
  4. When no new rows remain, create an empty CSV with headers (or skip writing) but still emit metadata signaling `no_new_reviews`.
  5. Ensure missing credentials trigger a logged warning and skip incremental filtering.
- **`scripts/03_upload_to_supabase.py`**
  1. Skip `upsert` calls for empty CSVs, logging `"no new reviews"` messages for traceability.
  2. Optionally consume the new metadata file(s) to short-circuit processing when every entry reports `status == "no_new_reviews"`.
- **Shared utilities (new module)**
  - Create `scripts/utils_supabase.py` (or similar) housing connection creation, pagination helper, and optional caching logic so both processing and upload steps reuse it.
- **Metadata/log outputs**
  - Amend run summaries to include the new fields and add a top-level run report capturing totals, per-combination statuses, and Supabase query timing metrics.
- **Documentation**
  - Update `AUDIT.md`/README-equivalent once implementation occurs to describe incremental behavior (outside this planning scope).

## 💡 Optimization Options
- **Caching**: Persist recently seen IDs per app-country in `data/cache/ids_{app}_{country}.json` with timestamps and reuse if the raw scrape date is <= 24h old, refreshing via Supabase when stale.
- **Pagination tuning**: Allow the helper to adapt batch size (500–2000) based on Supabase response times observed via timing logs written alongside metadata.
- **Supabase view**: Create a lightweight materialized view or function that returns hashes of recent IDs per combination to minimize transfer volume if tables exceed 100k rows.

## 🧾 Validation Plan
1. **Unit-style tests**: Mock Supabase client responses to validate pagination and filtering logic for `get_existing_review_ids` and the cleaning skip path.
2. **Dry-run pipeline**: Run `02_process_reviews.py` against a raw file with mixed existing/new IDs and verify summary metrics and output CSV contents.
3. **Supabase integration**: Execute `03_upload_to_supabase.py` with `SUPABASE_SERVICE_ROLE_KEY` in place, ensuring upsert is skipped when CSVs are empty and verifying log output.
4. **CI adjustments**: Update `.github/workflows/pipeline.yml` to surface the new metadata artifacts and optionally short-circuit upload step when no new reviews are reported.

---

*Generated automatically by Codex — 2025-11-08*
