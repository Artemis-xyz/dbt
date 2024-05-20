{{ config(materialized="table") }}

select *, 'ethereum' as chain, 'sweth' as app, 'DeFi' as category 
from (
    {{ calc_staked_eth('fact_sweth_staked_eth_count') }}
)
