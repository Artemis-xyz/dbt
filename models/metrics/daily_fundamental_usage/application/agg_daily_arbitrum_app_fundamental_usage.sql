-- depends_on {{ ref("fact_arbitrum_transactions_gold") }}
{{ config(materialized="table", snowflake_warehouse="DAILY_BAM") }}
{{ fundamental_data_by_app("fact_arbitrum_transactions_gold") }}
