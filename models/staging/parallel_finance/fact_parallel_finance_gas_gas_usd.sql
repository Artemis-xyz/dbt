{{ config(materialized="view", snowflake_warehouse="PARALLEL_FINANCE") }}

with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_parallel_finance_gas") }}
    ),
    raw as (
        select date(value:date) as date, value:"value"::float as gas
        from
            {{ source("PROD_LANDING", "raw_parallel_finance_gas") }},
            lateral flatten(input => parse_json(source_json))
        where extraction_date = (select max_date from max_extraction)
    ),
    prices as ({{ get_coingecko_price_with_latest("ethereum") }})
select raw.date, 'parallel_finance' as chain, gas, gas * coalesce(price, 0) as gas_usd
from raw
left join prices on raw.date = prices.date
where raw.date < to_date(sysdate())
