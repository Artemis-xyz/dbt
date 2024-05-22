{{ config(materialized="table") }}

select *, 'arbitrum' as chain, 'renzo_protocol' as app, 'DeFi' as category 
from (
    {{ calc_staked_eth('fact_renzo_protocol_arbitrum_restaked_eth_count', is_restaking=true) }}
)
