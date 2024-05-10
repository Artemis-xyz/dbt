{{
    config(
        materialized="table",
        snowflake_warehouse="JUPITER",
        database="jupiter",
        schema="core",
        alias="ez_metrics",
    )
}}

with
    fees_data as (
        select date, fees
        from {{ ref("fact_jupiter_fees_silver") }}
    ),
    perps_data as (
        select date, volume, traders from {{ ref("fact_jupiter_perps_silver") }}
    ),
    price_data as ({{ get_coingecko_metrics("jupiter-exchange-solana") }})
select
    fees_data.date,
    'solana' as chain,
    'jupiter' as protocol,
    volume as trading_volume,
    traders as unique_traders,
    fees,
    price,
    market_cap,
    fdmc
from fees_data
left join perps_data on fees_data.date = perps_data.date
left join price_data on fees_data.date = price_data.date
where fees_data.date < to_date(sysdate())
