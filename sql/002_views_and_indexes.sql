-- Aggregate sentiment or review counts per day
create or replace view v_reviews_daily_stats as
select
  app_name,
  country,
  date_trunc('day', review_date) as day,
  count(*) as reviews_count,
  avg(rating) as avg_rating
from clean_reviews
group by 1,2,3;
