-- depends_on {{ ref("fact_bsc_transactions_gold") }}
{{ config(materialized="table", snowflake_warehouse="DAILY_BAM_LG") }}
{{ fundamental_data_by_chain("bsc") }}
