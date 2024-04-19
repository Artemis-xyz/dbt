-- depends_on {{ ref("fact_blast_transactions_gold") }}
{{ config(materialized="table", snowflake_warehouse="DAILY_BAM") }}
{{ fundamental_data_by_chain("blast") }}
