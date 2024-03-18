{{ config(materialized="table") }}
select
    regexp_replace(
        regexp_substr(source_url, 'usernames=[^&]*'), 'usernames=', ''
    ) as twitter_handle,
    extraction_date::date as date,
    parse_json(max_by(source_json, extraction_date)):"data"[
        0
    ]:"public_metrics"."followers_count"::int as follower_count
from {{ source("PROD_LANDING", "raw_twitter_followers") }}
group by source_url, date
