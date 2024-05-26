{{ config(materialized="table") }}

select *, 'ethereum' as chain, 'bedrock' as app, 'DeFi' as category 
from (
    {{ calc_staked_eth('fact_bedrock_restaked_eth_count', is_restaking=true) }}
)
