{{config(materialized='table')}}
-- BOOMER
select from_address as address, array_agg(distinct symbol) as symbols, max(stablecoin_supply) as max_stablecoin_supply
from {{ref('ez_base_stablecoin_metrics_by_address_with_labels')}}
where date > '2023-12-31'
group by 1
