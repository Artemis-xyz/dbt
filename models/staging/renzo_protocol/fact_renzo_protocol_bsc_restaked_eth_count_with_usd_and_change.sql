{{ config(materialized="table") }}

select *, 'bsc' as chain, 'renzo_protocol' as app, 'DeFi' as category 
from (
    {{ calc_staked_eth('fact_renzo_protocol_bsc_restaked_eth_count') }}
)
