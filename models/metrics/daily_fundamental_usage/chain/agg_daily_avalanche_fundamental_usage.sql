-- depends_on {{ ref("ez_avalanche_transactions") }}
-- depends_on {{ ref("fact_avalanche_daily_balances") }}
{{ config(materialized="table", snowflake_warehouse="DAILY_BAM") }}
{{ get_fundamental_data_for_chain("avalanche") }}
