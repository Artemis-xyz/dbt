{{ config(materialized="table") }}

select *, 'ethereum' as chain, 'rsweth' as app, 'DeFi' as category 
from (
    {{ calc_staked_eth('fact_rsweth_restaked_eth_count', is_restaking=true) }}
)
