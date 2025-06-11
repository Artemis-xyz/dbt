{{
    config(
        materialized="table",
        snowflake_warehouse="CHAINLINK",
        database="chainlink",
        schema="raw",
        alias="fact_chainlink_daily_dau",
    )
}}

with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_chainlink_dau" ) }}
        
    ),
   latest_data as (
        select parse_json(source_json) as data
        from {{ source("PROD_LANDING", "raw_chainlink_dau") }}
        where extraction_date = (select max_date from max_extraction)
    ),
    flattened_data as (
        select
            f.value:day::date as date,
            f.value:total_unique_requesters::number as dau
        from latest_data, lateral flatten(input => data) as f
    )
select date, dau
from flattened_data
where date < to_date(sysdate())
order by date desc