{{
    config(
        materialized="table",
        snowflake_warehouse="GLOW",
    )
}}

with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_glow_revenue")}}
        
    ),
   latest_data as (
        select parse_json(source_json) as data
        from {{ source("PROD_LANDING", "raw_glow_revenue")}}
        where extraction_date = (select max_date from max_extraction)
    )
    , flattened_data as (
        select
            TO_TIMESTAMP_NTZ(f.value:date)::date as date,
            f.value:revenueUSD::number as fees
        from latest_data, lateral flatten(input => data:allProtocolFees) as f
    )
select date, fees
from flattened_data
where date < to_date(sysdate())
order by date desc