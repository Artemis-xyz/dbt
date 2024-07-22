{{ config(materialized="view") }}
with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_polkadot_daa_txns_gas_gas_usd_revenue") }}
    ),
    polkadot_data as (
        select parse_json(source_json) as data
        from {{ source("PROD_LANDING", "raw_polkadot_daa_txns_gas_gas_usd_revenue") }}
        where extraction_date = (select max_date from max_extraction)
    ),
    prices as ({{ get_coingecko_price_with_latest("polkadot") }}),
    polkadot_daa_txns_gas_gas_usd_revenue as (
        select
            to_timestamp(value:date::number / 1000)::date as date,
            value:daa daa,
            value:txns txns,
            value:gas gas,
            value:gas_usd gas_usd,
            value:revenue revenue,
            'polkadot' as chain
        from polkadot_data, lateral flatten(input => data)

    )
select
    t1.date,
    daa,
    txns,
    gas,
    cast(coalesce(nullifzero(gas_usd), gas * price, 0) as float) as gas_usd,
    cast(coalesce(nullifzero(revenue), gas * price * .8, 0) as float) as revenue,
    chain
from polkadot_daa_txns_gas_gas_usd_revenue t1
left join prices on prices.date = t1.date
order by date desc
