{{
    config(
        materialized="table",
        snowflake_warehouse="RESERVE",
    )
}}

with max_extraction as (
    select max(extraction_date) as max_date
    from {{ source("PROD_LANDING", "raw_reserve_dau") }}
)
, latest_data as (
    select parse_json(source_json) as data
    from {{ source("PROD_LANDING", "raw_reserve_dau") }}
    where extraction_date = (select max_date from max_extraction)
)
select
    left(f.value:time::string, 10)::date as date,
    f.value:users::number as dau
from latest_data, lateral flatten(input => data) f
