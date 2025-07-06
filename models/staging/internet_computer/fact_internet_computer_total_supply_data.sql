{{ config(materialized="table", snowflake_warehouse="INTERNET_COMPUTER") }}

with
    latest_source_json as (
        select extraction_date, source_url, source_json
        from {{ source("PROD_LANDING", "raw_icp_total_supply_data") }}
        order by extraction_date desc
        limit 1
    ),

    extracted_total_supply as (
        select
            to_date(to_timestamp_ntz(value[0]::number)) as date,
            value[1]::double / 1e8 as total_supply_native
        from latest_source_json, lateral flatten(input => parse_json(source_json))
    )
select
    date
    , total_supply_native
    , 'internet_computer' as chain
from extracted_total_supply
