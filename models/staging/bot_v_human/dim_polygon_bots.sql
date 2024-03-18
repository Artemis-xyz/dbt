-- depends_on: {{ ref('fact_polygon_daily_sleep') }}
{{ config(materialized="table", snowflake_warehouse="BAM_TRANSACTION_MD") }}
{{
    dim_bots(
        "polygon",
        "fact_polygon_daily_sleep",
    )
}}
