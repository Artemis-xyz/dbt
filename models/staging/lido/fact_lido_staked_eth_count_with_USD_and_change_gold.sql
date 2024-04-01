{{ config(materialized="table") }}
select *, 'ethereum' as chain, 'lido' as app, 'DeFi' as category
from {{ ref("fact_lido_staked_eth_count_with_USD_and_change") }}
