{{ config(materialized="table", snowflake_warehouse="HYPERLIQUID") }}

with latest_source_json as (
    select extraction_date, source_url, source_json
    from {{ source("PROD_LANDING", "raw_hyperliquid_new_users") }}
    where extraction_date = (select max(extraction_date) from {{ source("PROD_LANDING", "raw_hyperliquid_new_users") }})
),

extracted_new_users as (
    select
        value:cumulative_new_users::number as cumulative_new_users
        , value:daily_new_users::number as new_users
        , value:time as timestamp
    from latest_source_json, lateral flatten(input => parse_json(source_json))
)

select
    date(timestamp) as date,
    cumulative_new_users
    , new_users
    , 'hyperliquid' as chain
from extracted_new_users