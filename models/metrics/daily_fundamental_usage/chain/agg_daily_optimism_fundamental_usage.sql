-- depends_on {{ ref("ez_optimism_transactions") }}
-- depends_on {{ ref("fact_optimism_daily_balances") }}
{{ config(materialized="table", snowflake_warehouse="DAILY_BAM") }}
{{ get_fundamental_data_for_chain("optimism") }}
