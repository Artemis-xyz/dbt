{{ config(materialized="table") }}
select date, chain, 'uwulend' as app, category, daily_borrows_usd, daily_supply_usd
from {{ ref("fact_uwu_lend_ethereum_borrows_deposits") }}
