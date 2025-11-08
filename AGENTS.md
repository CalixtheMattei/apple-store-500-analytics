# AGENTS.md — Agent brief for Data Pipeline

## Project identity

* **Name:** Data Pipeline — App Review Insights
* **Mission:** Collect, clean, and analyze daily App Store reviews for HalfwayThere’s target apps to extract customer sentiment and feature insights.
* **Owner:** Calixthe
* **Context:** The pipeline supports the *HalfwayThere* project by automating app review scraping, cleaning, and enrichment for ML-based sentiment and topic analysis.

---

## Runtime overview

| Layer          | Description                                   | Folder               |
| -------------- | --------------------------------------------- | -------------------- |
| **Config**     | Defines scraping scope and runtime parameters | `config/`            |
| **Scripts**    | Core ETL steps: scrape → process → upload     | `scripts/`           |
| **Data**       | Temporary, gitignored, holds run outputs      | `data/`              |
| **SQL**        | Supabase schema & view definitions            | `sql/`               |
| **Automation** | GitHub Actions workflow definitions           | `.github/workflows/` |
| **Notebooks**  | Local exploration and modeling (manual runs)  | `notebooks/`         |

---

## Core objectives

1. **Scrape app reviews daily**

   * Source: App Store (via `app_store_web_scraper`)
   * Apps and countries defined in `config/apps.json`
   * Saves one CSV per app-country-date in `/data/raw/`
2. **Clean and normalize text**

   * Removes emojis, URLs, duplicates
   * Detects language
   * Outputs `/data/processed/*.csv`
3. **Upload cleaned data to Supabase**

   * Table: `clean_reviews`
   * Columns: app_name, country, rating, content, language, cleaned_content, review_date, source_review_id
4. **Run scheduled automation**

   * Daily cron via GitHub Actions (`.github/workflows/pipeline.yml`)
   * Logs and run artifacts saved per execution
5. **Prepare for ML enrichment**

   * Later stages add `sentiment_score`, topic clustering, and dashboards.

---

## Runtime / commands

### Local development

```bash
# Create venv and install dependencies
python -m venv .venv
source .venv/bin/activate        # or .venv\Scripts\activate on Windows
pip install -r requirements.txt

# Run the scraping pipeline
python scripts/01_scrape_reviews.py

# Clean and process reviews
python scripts/02_process_reviews.py

# Upload to Supabase
export SUPABASE_URL=...
export SUPABASE_SERVICE_ROLE_KEY=...
python scripts/03_upload_to_supabase.py
```

### Automated (CI)

Runs automatically via GitHub Actions:

```
.github/workflows/pipeline.yml
```

* Schedule: daily at 06:00 UTC
* Steps: install → scrape → process → upload → upload logs
* Secrets: `SUPABASE_URL`, `SUPABASE_SERVICE_ROLE_KEY`

---

## Tech stack & dependencies

* **Python 3.11**
* **Supabase (Postgres + Storage)** for cloud persistence
* **GitHub Actions** for orchestration
* **Libraries:**

  * `app_store_web_scraper`
  * `pandas`
  * `langdetect`
  * `emoji`
  * `beautifulsoup4`
  * `supabase-py`
  * `tqdm`

---

## Key configuration files

| File                             | Purpose                                  |
| -------------------------------- | ---------------------------------------- |
| `config/apps.json`               | List of app IDs, countries, scrape delay |
| `requirements.txt`               | Python dependencies                      |
| `sql/001_create_tables.sql`      | Creates `clean_reviews` table            |
| `sql/002_views_and_indexes.sql`  | Aggregation views                        |
| `.github/workflows/pipeline.yml` | Automated daily pipeline                 |
| `.gitignore`                     | Prevents committing data/logs            |

---

## Data flow

```mermaid
graph LR
A[GitHub Action (cron)] --> B[scripts/01_scrape_reviews.py]
B --> C[scripts/02_process_reviews.py]
C --> D[scripts/03_upload_to_supabase.py]
D --> E[(Supabase: clean_reviews table)]
E --> F[Analysis notebooks / ML pipeline]
```

---

## Agent responsibilities

| Agent               | Role                   | Tasks                                                                                               |
| ------------------- | ---------------------- | --------------------------------------------------------------------------------------------------- |
| **Scraper Agent**   | Data extraction        | Run daily job to fetch reviews via `AppStoreEntry`. Save structured CSV snapshots and summary JSON. |
| **Processor Agent** | Data cleaning          | Normalize text, detect language, deduplicate, and prepare data for upload.                          |
| **Uploader Agent**  | Data persistence       | Upsert cleaned rows into `clean_reviews` in Supabase using `SERVICE_ROLE` credentials.              |
| **Analyst Agent**   | ML enrichment (future) | Run sentiment and topic modeling notebooks, write results to `review_insights`.                     |

---

## Notes for Codex agents

* All persistent data should go to Supabase — **never commit data/** outputs.
* Config files (`config/apps.json`, `config/settings.json`) define runtime scope.
* Logs and CSVs under `/data/` are **temporary artifacts** (gitignored).
* When writing code, prefer `Path` objects and environment variables for portability.
* For analysis tasks, use the `clean_reviews` table as the canonical dataset.
* For automation tasks, edit `.github/workflows/pipeline.yml`.

---

## Future extensions

* Add `raw_reviews` and `run_logs` tables to Supabase for traceability.
* Add sentiment analysis in `scripts/04_analyze_sentiment.py`.
* Create dashboard views (`review_insights`, `v_sentiment_trends`).
* Integrate Supabase Storage for archived raw JSON files.
* Add Slack webhook notifications for failed runs.

---

*Document version:* `v1.0`
*Last updated:* `{{DATE}}`
*Maintainer:* Calixthe
