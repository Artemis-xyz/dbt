-- depends_on: {{ ref('fact_avalanche_daily_sleep') }}
{{ config(materialized="table", snowflake_warehouse="BAM_TRANSACTION_SM") }}
{{ dim_bots("avalanche", "fact_avalanche_daily_sleep") }}
