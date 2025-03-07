{{
    config(
        materialized="table",
        snowflake_warehouse="VIRTUALS",
    )
}}

with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_virtuals_daily_agents_created") }}
        
    ),
    latest_data as (
        select parse_json(source_json) as data
        from {{ source("PROD_LANDING", "raw_virtuals_daily_agents_created") }}
        where extraction_date = (select max_date from max_extraction)
    )
select
    left(f.value:period::string, 10)::date as date,
    f.value:launched::number as daily_agents,
    'virtuals' as app,
    'base' as chain,
    'DeFi' as category
from latest_data, lateral flatten(input => data) f
