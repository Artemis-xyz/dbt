{{ config(materialized="table", snowflake_warehouse="MOONWELL") }}
select date, chain, app, category, daily_borrows_usd, daily_supply_usd
from {{ ref("fact_moonwell_base_borrows_deposits") }}
