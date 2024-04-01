-- depends_on: {{ ref('fact_bsc_daily_sleep') }}
{{ config(materialized="table", snowflake_warehouse="BAM_TRANSACTION_MD") }}
{{
    dim_bots(
        "bsc",
        "fact_bsc_daily_sleep",
    )
}}
