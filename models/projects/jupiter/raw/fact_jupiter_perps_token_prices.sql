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
    tx_id as tx_hash,
    chain,
    app,
    symbol,
    price,
    mint as token_address
FROM {{ ref('fact_jupiter_perps_txs') }} t
LEFT JOIN solana_flipside.price.ez_asset_metadata m ON m.token_address = t.mint