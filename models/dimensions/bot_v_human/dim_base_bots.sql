-- depends_on: {{ ref('fact_base_daily_sleep') }}
{{ config(materialized="table", snowflake_warehouse="BAM_TRANSACTION_SM") }}
{{
    dim_bots(
        "base",
        "fact_base_daily_sleep",
    )
}}
