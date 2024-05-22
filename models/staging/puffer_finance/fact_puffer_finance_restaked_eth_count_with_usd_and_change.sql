{{ config(materialized="table") }}

select *, 'ethereum' as chain, 'puffer_finance' as app, 'DeFi' as category 
from (
    {{ calc_staked_eth('fact_puffer_finance_restaked_eth_count', is_restaking=true) }}
)
