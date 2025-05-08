-- depends_on: {{ ref('fact_blast_daily_sleep') }}
{{ config(materialized="table", snowflake_warehouse="BAM_TRANSACTION_SM", enabled=false) }}
{{
    dim_bots(
        "blast",
        "fact_blast_daily_sleep",
    )
}}
