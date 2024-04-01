-- depends_on {{ ref("fact_base_transactions_gold") }}
{{ config(materialized="table", snowflake_warehouse="DAILY_BAM") }}
{{ fundamental_data_by_category("fact_base_transactions_gold") }}
