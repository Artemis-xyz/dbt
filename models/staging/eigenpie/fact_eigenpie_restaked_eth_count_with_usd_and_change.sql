{{ config(materialized="table") }}

select *, 'ethereum' as chain, 'eigenpie-restaked-eth' as app, 'DeFi' as category 
from (
    {{ calc_staked_eth('fact_eigenpie_restaked_eth_count', is_restaking=true) }}
)
