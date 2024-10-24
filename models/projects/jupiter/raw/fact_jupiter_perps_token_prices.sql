{{
    config(
        materialized="table",
        snowflake_warehouse="JUPITER",
        database="jupiter",
        schema="raw",
        alias="fact_perps_token_prices",
    )
}}

SELECT
    block_timestamp,
    tx_id,
    chain,
    app,
    mint,
    price,
    size_usd,
    fee_usd
FROM {{ ref('fact_jupiter_perps_txs') }}