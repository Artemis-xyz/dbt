-- depends_on: {{ ref('fact_arbitrum_daily_sleep') }}
{{ config(materialized="table", snowflake_warehouse="BAM_TRANSACTION_SM") }}
{{
    dim_bots(
        "arbitrum",
        "fact_arbitrum_daily_sleep",
    )
}}
