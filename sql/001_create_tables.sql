-- Base table for cleaned reviews
create table if not exists clean_reviews (
  id bigserial primary key,
  app_name text not null,
  country text not null,
  source text not null default 'app_store',
  source_review_id text not null,
  rating int check (rating between 1 and 5),
  title text,
  content text,
  language text,
  cleaned_content text,
  review_date date,
  inserted_at timestamptz default now()
);

-- Prevent duplicates on re-scrapes
create unique index if not exists uq_clean_reviews_unique
  on clean_reviews (source, app_name, country, source_review_id);
