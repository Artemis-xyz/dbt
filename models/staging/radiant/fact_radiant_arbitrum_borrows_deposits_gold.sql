{{ config(materialized="table", snowflake_warehouse="RADIANT") }}
select date, chain, 'radiant' as app, category, daily_borrows_usd, daily_supply_usd
from {{ ref("fact_radiant_v2_arbitrum_borrows_deposits") }}
