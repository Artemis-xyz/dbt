-- depends_on {{ ref("fact_near_transactions_gold") }}
{{ config(materialized="table", snowflake_warehouse="DAILY_BAM_MD") }}
{{ fundamental_data_by_chain("near") }}
