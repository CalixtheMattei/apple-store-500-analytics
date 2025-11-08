# scripts/01_scrape_reviews.py
import json, time
from datetime import datetime
from pathlib import Path
import pandas as pd
from app_store_web_scraper import AppStoreEntry

CONFIG_PATH = Path("config/apps.json")
BASE = Path("data/raw")
BASE.mkdir(parents=True, exist_ok=True)

def load_config():
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        cfg = json.load(f)
    return cfg["apps"], cfg["countries"], cfg.get("source", "app_store"), cfg.get("scrape_delay_seconds", 2)

def scrape_app_reviews(app, country, source):
    app_entry = AppStoreEntry(app_id=app["id"], country=country)
    rows = []
    for i, r in enumerate(app_entry.reviews(), start=1):
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
