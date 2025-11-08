CREATE TABLE IF NOT EXISTS clean_reviews (
  id BIGSERIAL PRIMARY KEY,
  app_name TEXT NOT NULL,
  country TEXT NOT NULL,
  source TEXT NOT NULL DEFAULT 'app_store',
  source_review_id TEXT NOT NULL,
  rating INT CHECK (rating BETWEEN 1 AND 5),
  title TEXT,
  content TEXT,
  cleaned_content TEXT,
  language TEXT,
  review_date DATE,
  inserted_at TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE (source, app_name, country, source_review_id)
);
