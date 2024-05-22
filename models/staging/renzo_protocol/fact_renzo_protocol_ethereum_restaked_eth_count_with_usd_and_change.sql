{{ config(materialized="table") }}

select *, 'ethereum' as chain, 'renzo_protocol' as app, 'DeFi' as category 
from (
    {{ calc_staked_eth('fact_renzo_protocol_ethereum_restaked_eth_count') }}
)
