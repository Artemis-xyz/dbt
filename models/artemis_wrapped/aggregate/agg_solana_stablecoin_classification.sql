{{config(materialized='table')}}

select from_address as address, array_agg(distinct symbol) as symbols, max(stablecoin_supply) as max_stablecoin_supply
from {{ref('ez_solana_stablecoin_metrics_by_address')}}
where date > '2023-12-31'
group by 1
