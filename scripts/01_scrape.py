# scripts/01_scrape_reviews.py
import json, time
from datetime import datetime
from pathlib import Path

import pandas as pd
from app_store_web_scraper import AppStoreEntry
from app_store_web_scraper._errors import AppNotFound

CONFIG_PATH = Path("config/apps.json")
BASE = Path("data/raw")
BASE.mkdir(parents=True, exist_ok=True)

def load_config():
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        cfg = json.load(f)
    return cfg["apps"], cfg["countries"], cfg.get("source", "app_store"), cfg.get("scrape_delay_seconds", 2)

def _iter_reviews(app_entry, limit=None):
    """Iterate reviews while handling feeds that return a dict instead of a list."""
    limit = app_entry.MAX_REVIEWS_LIMIT if limit is None else limit
    if limit <= 0:
        raise ValueError("Limit must be positive")
    limit = min(limit, app_entry.MAX_REVIEWS_LIMIT)
    count = 0
    for page in range(1, app_entry._REVIEWS_FEED_PAGE_LIMIT + 1):
        path = f"/{app_entry.country}/rss/customerreviews/page={page}/id={app_entry.app_id}/sortby=mostrecent/json"
        data = app_entry._session._get(path)
        feed = data.get("feed", {})
        links = feed.get("link", [])
        app_exists = any(
            isinstance(link, dict)
            and link.get("attributes", {}).get("rel") == "self"
            for link in links
        )
        if not app_exists:
            raise AppNotFound(app_entry.app_id, app_entry.country)
        entries = feed.get("entry")
        if not entries:
            return
        if isinstance(entries, dict):
            entries = [entries]
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            try:
                review = app_entry._parse_review_entry(entry)
            except (KeyError, TypeError):
                # Skip malformed entries such as the metadata record
                continue
            yield review
            count += 1
            if count == limit:
                return


def scrape_app_reviews(app, country, source):
    app_entry = AppStoreEntry(app_id=app["id"], country=country)
    rows = []
    for i, r in enumerate(_iter_reviews(app_entry), start=1):
        try:
            d = r.date
            if hasattr(d, "tzinfo") and d.tzinfo is not None:
                d = d.astimezone(None).replace(tzinfo=None)
            rows.append({
                "source_review_id": r.id,
                "title": r.title,
                "rating": r.rating,
                "review_date": d,
                "content": r.content,
                "country": country,
                "app_name": app["name"],
                "source": source
            })
            if i % 100 == 0:
                print(f"   🔹 {i} reviews fetched...")
        except Exception as e:
            print(f"❌ Error parsing review: {e}")
    return pd.DataFrame(rows)

def main():
    apps, countries, source, delay = load_config()
    print(f"🧩 Loaded {len(apps)} apps and {len(countries)} countries from config/apps.json\n")
    all_data = []
    for country in countries:
        for app in apps:
            print(f"🌍 {app['name']} ({country})")
            df = scrape_app_reviews(app, country, source)
            if df.empty:
                print("⚠️ No reviews found.")
                continue
            filename = f"{app['name']}_{country}_{datetime.now().date()}.csv"
            out_path = BASE / filename
            df.to_csv(out_path, index=False)
            print(f"💾 Saved {len(df)} reviews → {out_path}")
            all_data.append(df)
            time.sleep(delay)
    total = sum(len(df) for df in all_data)
    summary = {
        "timestamp": datetime.now().isoformat(),
        "total_reviews_fetched": total,
        "apps_scraped": len(apps),
        "countries_scraped": len(countries)
    }
    summary_path = BASE / f"run_summary_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.json"
    summary_path.write_text(json.dumps(summary, indent=2))
    print(f"\n📊 Summary: {summary}")

if __name__ == "__main__":
    main()
