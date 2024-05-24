{{ config(materialized="table") }}

select *, 'ethereum' as chain, 'ethx' as app, 'DeFi' as category 
from (
    {{ calc_staked_eth('fact_ethx_staked_eth_count') }}
)
