{{
    config(
        materialized="table",
        snowflake_warehouse="ORCA",
    )
}}

with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_orca_fees_and_volume") }}
    )
, latest_data as (
        select parse_json(source_json) as data
        from {{ source("PROD_LANDING", "raw_orca_fees_and_volume") }}
        where extraction_date = (select max_date from max_extraction)
    )
select
    f.value:date::date as date,
    f.value:climate_fund_fees::number as climate_fund_fees,
    f.value:dao_treasury_fees::number as dao_treasury_fees,
    f.value:lp_fees::number as lp_fees,
    f.value:total_fees::number as total_fees,
    f.value:volume::number as volume
from latest_data, lateral flatten(input => data) f