{{ config(materialized="table") }}

select *, 'ethereum' as chain, 'etherfi' as app, 'DeFi' as category 
from (
    {{ calc_staked_eth('fact_etherfi_restaked_eth_count') }}
)
