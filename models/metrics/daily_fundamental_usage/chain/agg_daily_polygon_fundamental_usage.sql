-- depends_on {{ ref("ez_polygon_transactions") }}
-- depends_on {{ ref("fact_polygon_daily_balances") }}
{{ config(materialized="table", snowflake_warehouse="DAILY_BAM_MD") }}
{{ get_fundamental_data_for_chain("polygon") }}
