-- depends_on: {{ ref('fact_optimism_daily_sleep') }}
{{ config(materialized="table", snowflake_warehouse="BAM_TRANSACTION_SM") }}
{{
    dim_bots(
        "optimism",
        "fact_optimism_daily_sleep",
    )
}}
