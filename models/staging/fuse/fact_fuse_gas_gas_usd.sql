{{ config(materialized="view", snowflake_warehouse="FUSE") }}
with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_fuse_gas") }}
    ),
    raw as (
        select date(value:date) as date, value:"value"::float as gas
        from
            {{ source("PROD_LANDING", "raw_fuse_gas") }},
            lateral flatten(input => parse_json(source_json))
        where extraction_date = (select max_date from max_extraction)
    ),
    price as ({{ get_coingecko_price_with_latest("fuse-network-token") }}),
    data as (
        select raw.date, 'fuse' as chain, gas, gas * coalesce(price.price, 0) as gas_usd
        from raw
        left join price on raw.date = price.date
        where raw.date < to_date(sysdate())
    )
select date, chain, gas, gas_usd
from data
