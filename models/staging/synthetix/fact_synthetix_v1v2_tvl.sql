{{ config(snowflake_warehouse="SYNTHETIX", materialized="table") }}

with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_synthetix_total_value_locked") }}

    ),
    latest_data as (
        select parse_json(source_json) as data
        from {{ source("PROD_LANDING", "raw_synthetix_total_value_locked") }}
        where extraction_date = (select max_date from max_extraction)
    )
select
    left(f.value:day, 10)::date as date,
    --f.value:SDS_L1::number + f.value:SDS_L2::number  + f.value:TVL_L1_Staked::number + f.value:TVL_L2_Staked::number as tvl
    (f.value:TVL_L1_Staked::number + f.value:TVL_L2_Staked::number) as tvl
from latest_data, lateral flatten(input => data) f

