{{ config(materialized="table") }}

select *, 'ethereum' as chain, 'binance-staked-eth' as app, 'DeFi' as category 
from (
    {{ calc_staked_eth('fact_binance_staked_eth_count') }}
)
