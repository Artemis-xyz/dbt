-- depends_on {{ ref("fact_ethereum_transactions_gold") }}
{{ config(materialized="table", snowflake_warehouse="DAILY_BAM_MD") }}
{{ fundamental_data_by_app("fact_ethereum_transactions_gold") }}
