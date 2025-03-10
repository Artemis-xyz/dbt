{{
    config(
        materialized="table",
        snowflake_warehouse="GMX",
    )
}}

with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_gmx_fees_and_volume" ) }}
        
    ),
   latest_data as (
        select parse_json(source_json) as data
        from {{ source("PROD_LANDING", "raw_gmx_fees_and_volume") }}
        where extraction_date = (select max_date from max_extraction)
    ),
    flattened_data as (
        select
            left(f.value:block_date::string, 10)::date as date,
            f.value:"fees"::number as fees,
            f.value:"volume"::number as volume
        from latest_data, lateral flatten(input => data) as f
    )
select 
    date, 
    fees,
    fees * 0.3 as revenue,
    fees * 0.7 as supply_side_revenue,
    volume
from flattened_data
where date < to_date(sysdate())
order by date desc