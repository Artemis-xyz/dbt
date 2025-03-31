{{
    config(
        materialized="table",
        snowflake_warehouse="AETHIR",
    )
}}

with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_aethir_revenue") }}
        
    ),
   latest_data as (
        select parse_json(source_json) as data
        from {{ source("PROD_LANDING", "raw_aethir_revenue") }}
        where extraction_date = (select max_date from max_extraction)
    )
    , flattened_data as (
        select
            f.value:date::date as date,
            f.value:usdValue::number as compute_revenue
        from latest_data, lateral flatten(input => data:dailyRevenue) as f
    )
select date, compute_revenue
from flattened_data
where date < to_date(sysdate())
order by date desc