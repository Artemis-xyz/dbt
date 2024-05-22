{{ config(materialized="table") }}

select *, 'mode' as chain, 'renzo_protocol' as app, 'DeFi' as category 
from (
    {{ calc_staked_eth('fact_renzo_protocol_mode_restaked_eth_count', is_restaking=true) }}
)
