{{ config(materialized="view", snowflake_warehouse="ACALA") }}
with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_acala_daa_txns_gas_gas_usd_revenue") }}
    ),
    acala_data as (
        select parse_json(source_json) as data
        from {{ source("PROD_LANDING", "raw_acala_daa_txns_gas_gas_usd_revenue") }}
        where extraction_date = (select max_date from max_extraction)
    ),
    acala_daa_txns_gas_gas_usd_revenue as (
        select
            to_timestamp(value:date::number / 1000)::date as date,
            value:daa daa,
            value:txns txns,
            value:gas gas,
            value:gas_usd gas_usd,
            value:revenue revenue,
            'acala' as chain
        from acala_data, lateral flatten(input => data)
    )
select date, daa, txns, gas, gas_usd, revenue, chain
from acala_daa_txns_gas_gas_usd_revenue
