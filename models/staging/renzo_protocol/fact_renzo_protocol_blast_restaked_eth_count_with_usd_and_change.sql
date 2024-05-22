{{ config(materialized="table") }}

select *, 'blast' as chain, 'renzo_protocol' as app, 'DeFi' as category 
from (
    {{ calc_staked_eth('fact_renzo_protocol_blast_restaked_eth_count') }}
)
