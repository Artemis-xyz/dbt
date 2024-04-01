{{ config(materialized="table") }}
select *, 'ethereum' as chain, 'frax' as app, 'DeFi' as category
from {{ ref("fact_frax_staked_eth_count_with_USD_and_change") }}
