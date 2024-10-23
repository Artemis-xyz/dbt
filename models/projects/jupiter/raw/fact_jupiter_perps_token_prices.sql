{{
    config(
        materialized="table",
        snowflake_warehouse="JUPITER",
        database="jupiter",
        schema="raw",
        alias="fact_perps_token_prices",
    )
}}

SELECT * FROM {{ ref('fact_jupiter_perps_txs') }}