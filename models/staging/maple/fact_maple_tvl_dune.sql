{{
    config(
        materialized="table",
        snowflake_warehouse="MAPLE",
    )
}}

with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_maple_tvl_dune" ) }}
        
    ),
   latest_data as (
        select parse_json(source_json) as data
        from {{ source("PROD_LANDING", "raw_maple_tvl_dune") }}
        where extraction_date = (select max_date from max_extraction)
    ),
    flattened_data as (
        select
            f.value:date::date as date,
            f.value:pool_name::string as pool_name,
            f.value:tvl::number as tvl,
            f.value:outstanding::number as outstanding
        from latest_data, lateral flatten(input => data) as f
    )
select 
    date, 
    pool_name, 
    tvl, 
    outstanding
from flattened_data
where date < to_date(sysdate())
order by date desc