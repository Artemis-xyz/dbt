{{ config(materialized="table") }}
select *, 'ethereum' as chain, 'stakewise' as app, 'DeFi' as category
from {{ ref("fact_stakewise_staked_eth_count_with_USD_and_change") }}
