{{ config(materialized="table") }}
select date, chain, 'radiant' as app, category, daily_borrows_usd, daily_supply_usd
from {{ ref("fact_radiant_v2_arbitrum_borrows_deposits") }}
