{{
    config(
        materialized="table",
        snowflake_warehouse="MANTA"
    )
}}

with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_manta_daily_txns_and_daa") }}
    )
    , metrics as(
        select
            left(value:date, 10)::date as date,
            value:daily_txns::number as daily_txns,
            value:daa::number as dau,
            value:gas_native::number as gas_native
        from
            {{ source("PROD_LANDING", "raw_manta_daily_txns_and_daa") }},
            lateral flatten(input => parse_json(source_json))
        where extraction_date = (select max_date from max_extraction)
    )
select
    metrics.date,
    metrics.daily_txns,
    metrics.dau,
    metrics.gas_native * p.price as fees -- gas fees on Manta Pacific paid in ETH
from metrics
left join
    ({{ get_coingecko_price_with_latest("ethereum") }}) p using (date)
where metrics.date < to_date(sysdate())

