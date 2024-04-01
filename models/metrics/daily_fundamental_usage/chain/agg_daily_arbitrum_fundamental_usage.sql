-- depends_on {{ ref("fact_arbitrum_transactions_gold") }}
-- depends_on {{ ref("fact_arbitrum_daily_balances") }}
{{ config(materialized="table", snowflake_warehouse="DAILY_BAM") }}
{{ fundamental_data_by_chain("arbitrum") }}
