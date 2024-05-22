{{ config(materialized="table") }}

select *, 'base' as chain, 'renzo_protocol' as app, 'DeFi' as category 
from (
    {{ calc_staked_eth('fact_renzo_protocol_base_restaked_eth_count') }}
)
