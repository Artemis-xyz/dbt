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
        select date, volume, traders, txns
        from {{ ref("fact_jupiter_perps_silver")}}
    ),
    aggregator_data as (
        select date, aggregator_multi_hop_volume, aggregator_single_hop_volume, unique_aggregator_traders
        from {{ ref("fact_jupiter_aggregator_stats")}}
    ),
    price_data as ({{ get_coingecko_metrics("jupiter-exchange-solana") }})
select
    fees_data.date as date,
    'solana' as chain,
    'jupiter' as protocol,
    fees_data.fees as fees,
    volume as trading_volume,
    traders as unique_traders,
    txns,
    aggregator_data.aggregator_multi_hop_volume as aggregator_multi_hop_volume,
    aggregator_data.aggregator_single_hop_volume as aggregator_single_hop_volume,
    aggregator_data.unique_aggregator_traders as aggregator_unique_traders,
    price,
    market_cap,
    fdmc
from fees_data
left join perps_data using (date)
left join price_data using (date)
left join aggregator_data using (date)
where fees_data.date < to_date(sysdate())