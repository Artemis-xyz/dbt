{{ config(materialized="table") }}

select
    date_trunc('day', block_timestamp) as date,
    coalesce(sum(trading_volume), 0) as trading_volume,
    count(distinct trader) as unique_traders,
    chain as chain,
    'mux' as app,
    'DeFi' as category
from {{ ref("fact_mux_raw_events") }}
group by date, chain
