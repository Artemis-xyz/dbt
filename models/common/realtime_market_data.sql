{{
    config(
        snowflake_warehouse="COMMON",
        database="common",
        schema="core",
        materialized='table'
    )
}}


SELECT *
FROM
    {{ source("PC_DBT_DB_UPSTREAM", "fact_coingecko_token_realtime_data") }}
