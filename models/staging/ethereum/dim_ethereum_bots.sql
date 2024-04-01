-- depends_on: {{ ref('fact_ethereum_daily_sleep') }}
{{ config(materialized="table", snowflake_warehouse="BAM_TRANSACTION_MD") }}
{{
    dim_bots(
        "ethereum",
        "fact_ethereum_daily_sleep",
    )
}}
