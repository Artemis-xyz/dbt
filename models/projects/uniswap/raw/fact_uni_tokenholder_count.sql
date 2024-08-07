{{
    config(
        materialized="table",
        snowflake_warehouse="UNISWAP_SM",
        database="uniswap",
        schema="raw",
        alias="fact_tokenholder_count",
    )
}}

WITH tokenholder_counts AS (
    {{ get_daily_tokenholder_count_flipside('ethereum', '0x1f9840a85d5aF5bf1D1762F925BDADdC4201F984') }}
)
SELECT * FROM tokenholder_counts