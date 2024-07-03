{{ config(materialized="table") }}

select *, 'ethereum' as chain, 'cbeth' as app, 'DeFi' as category 
from (
    {{ calc_staked_eth('fact_coinbase_staked_eth_count') }}
)
