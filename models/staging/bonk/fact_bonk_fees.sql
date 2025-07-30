{{
    config(
        materialized='table',
        snowflake_warehouse='BONK',
    )
}}
with data as (
    select 
        value:"bonk_revenue_usd"::float as bonk_fees
        ,  date_trunc('day', TO_TIMESTAMP(REPLACE(value:"day"::string, ' UTC', '')))  as date
        ,  extraction_date
    from landing_database.prod_landing.raw_bonk_revenue
    , lateral flatten(source_json)
)
select 
    date
    , MAX_BY(bonk_fees, extraction_date) as bonk_fees
from data
group by date
