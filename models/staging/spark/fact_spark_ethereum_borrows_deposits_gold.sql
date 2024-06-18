{{ config(materialized="table", snowflake_warehouse="SPARK") }}
select date, chain, 'spark' as app, category, daily_borrows_usd, daily_supply_usd
from {{ ref("fact_spark_ethereum_borrows_deposits") }}
