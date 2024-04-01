{{ config(materialized="table") }}
with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_sui_data") }}
    ),
    sui_data as (
        select parse_json(source_json) as data
        from {{ source("PROD_LANDING", "raw_sui_data") }}
        where extraction_date = (select max_date from max_extraction)
    ),
    sui_prices as ({{ get_coingecko_price_with_latest("sui") }}),
    raw as (
        select
            date(value:day) as date,
            value:dau as daa,
            value:txns as txns,
            value:fees / 1e9 as gas,
            value:revs / 1e9 as revenue_native,
            value as source,
            'sui' as chain
        from sui_data, lateral flatten(input => data:data:records)
    )
select raw.*, gas * price as gas_usd, revenue_native * price as revenue
from raw
left join sui_prices on raw.date = sui_prices.date
