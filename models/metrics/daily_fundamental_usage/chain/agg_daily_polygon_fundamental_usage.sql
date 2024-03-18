-- depends_on {{ ref("fact_polygon_transactions_gold") }}
-- depends_on {{ ref("fact_polygon_daily_balances") }}
{{ config(materialized="table", snowflake_warehouse="DAILY_BAM_MD") }}
{{ fundamental_data_by_chain("polygon") }}
