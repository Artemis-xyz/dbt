{{ config(materialized="view", snowflake_warehouse="CARDANO") }}
with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_cardano_fees") }}
    ),
    cardano_prices as ({{ get_coingecko_price_with_latest("cardano") }}),
    raw as (
        select date(value:date) as date, value:"fees"::int as fees, value as source
        from
            {{ source("PROD_LANDING", "raw_cardano_fees") }},
            lateral flatten(input => parse_json(source_json))
        where extraction_date = (select max_date from max_extraction)
    )
select
    raw.date,
    'cardano' as chain,
    fees as gas,
    fees * coalesce(price, 0) as gas_usd,
    0 as revenue
from raw
left join cardano_prices on raw.date = cardano_prices.date
