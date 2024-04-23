{{ config(materialized="table") }}
select *, 'ethereum' as chain, 'meth' as app, 'DeFi' as category
from {{ ref("fact_meth_staked_eth_count_with_USD_and_change") }}
