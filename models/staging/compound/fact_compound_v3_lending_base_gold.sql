{{ config(materialized="table") }}
select date, chain, app, category, daily_borrows_usd, daily_supply_usd
from {{ ref("fact_compound_v3_lending_base") }}
