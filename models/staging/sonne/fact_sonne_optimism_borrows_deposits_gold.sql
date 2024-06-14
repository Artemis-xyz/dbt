{{ config(materialized="table", snowflake_warehouse="SONNE") }}
select date, chain, app, category, daily_borrows_usd, daily_supply_usd
from {{ ref("fact_sonne_optimism_borrows_deposits") }}
