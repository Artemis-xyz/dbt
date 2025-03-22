{{ config(snowflake_warehouse="LIFINITY", materialized="table") }}

with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_lifinity_dex_volumes") }}

    ),
    latest_data as (
        select parse_json(source_json) as data
        from {{ source("PROD_LANDING", "raw_lifinity_dex_volumes") }}
        where extraction_date = (select max_date from max_extraction)
    )
select
    left(f.value:date, 10)::date as date,
    f.value:daily_trading_volume::number as daily_volume
from latest_data, lateral flatten(input => data) f


