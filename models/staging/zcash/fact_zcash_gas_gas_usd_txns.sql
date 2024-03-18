{{ config(materialized="view") }}
with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_zcash_gas_txns") }}
    ),
    zcash_prices as ({{ get_coingecko_price_with_latest("zcash") }}),
    raw as (
        select
            date(value:date) as date,
            value:"gas"::float as gas,
            value:"txns"::int as txns
        from
            {{ source("PROD_LANDING", "raw_zcash_gas_txns") }},
            lateral flatten(input => parse_json(source_json))
        where extraction_date = (select max_date from max_extraction)
    )
select raw.date, 'zcash' as chain, gas, gas * coalesce(price, 0) as gas_usd, txns
from raw
left join zcash_prices on raw.date = zcash_prices.date
where raw.date < to_date(sysdate())
