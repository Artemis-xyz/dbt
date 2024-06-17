{{ config(materialized="table", snowflake_warehouse="VENUS") }}
select date, chain, 'venus' as app, category, daily_borrows_usd, daily_supply_usd
from {{ ref("fact_venus_v4_lending_bsc_gold") }}
