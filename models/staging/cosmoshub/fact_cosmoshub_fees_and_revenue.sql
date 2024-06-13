{{ config(materialized="view", snowflake_warehouse="COSMOSHUB") }}
with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_cosmoshub_fees") }}
    ),
    cosmoshub_prices as ({{ get_coingecko_price_with_latest("cosmos") }}),
    raw as (
        select date(value:date) as date, value:"fee"::float as fees, value:"fee_currency"::string as fee_currency
        from
            {{ source("PROD_LANDING", "raw_cosmoshub_fees") }},
            lateral flatten(input => parse_json(source_json))
        where extraction_date = (select max_date from max_extraction)
    )
select
    raw.date,
    'cosmoshub' as chain,
    sum(fees) as gas,
    sum(fees * coalesce(price, 0)) as gas_usd,
    gas_usd * .02 as revenue
from raw
left join cosmoshub_prices on raw.date = cosmoshub_prices.date and fee_currency = 'uatom'
group by 1
