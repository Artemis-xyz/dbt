{{ config(materialized="table") }}

select *, 'ethereum' as chain, 'kelp-dao-restaked-eth' as app, 'DeFi' as category 
from (
    {{ calc_staked_eth('fact_kelp_dao_restaked_eth_count', is_restaking=true) }}
)
