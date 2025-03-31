{{
    config(
        materialized="table",
        snowflake_warehouse="LIVEPEER",
    )
}}

with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_livepeer_revenue") }}
        
    ),
   latest_data as (
        select parse_json(source_json) as data
        from {{ source("PROD_LANDING", "raw_livepeer_revenue") }}
        where extraction_date = (select max_date from max_extraction)
    )
    , flattened_data as (
        select
            left(f.value:day, 10)::date as date,
            f.value:daily_fees_usd::number as fees
        from latest_data, lateral flatten(input => data) as f
    )
select date, fees
from flattened_data
where date < to_date(sysdate())
order by date desc