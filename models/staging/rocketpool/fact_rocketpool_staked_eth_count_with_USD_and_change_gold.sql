{{ config(materialized="table") }}
select *, 'ethereum' as chain, 'rocketpool' as app, 'DeFi' as category
from {{ ref("fact_rocketpool_staked_eth_count_with_USD_and_change") }}
