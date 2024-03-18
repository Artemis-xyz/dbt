-- depends_on {{ ref("fact_avalanche_transactions_gold") }}
-- depends_on {{ ref("fact_avalanche_daily_balances") }}
{{ config(materialized="table", snowflake_warehouse="DAILY_BAM") }}
{{ fundamental_data_by_chain("avalanche") }}
