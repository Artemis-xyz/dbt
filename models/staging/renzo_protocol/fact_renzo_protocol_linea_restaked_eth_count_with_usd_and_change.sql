{{ config(materialized="table") }}

select *, 'linea' as chain, 'renzo_protocol' as app, 'DeFi' as category 
from (
    {{ calc_staked_eth('fact_renzo_protocol_linea_restaked_eth_count') }}
)
