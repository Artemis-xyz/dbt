{{
    config(
        materialized="table",
        snowflake_warehouse="MAGICEDEN"
    )
}}

SELECT 
    DATE(BLOCK_TIMESTAMP) AS date,
    tx_hash,
    'ethereum' AS chain,
    PRICE_USD AS daily_trading_volume_usd,
    CAST(2 AS NUMERIC) AS active_wallets,
    CAST(1 AS NUMERIC) AS collections_transacted,
    CAST(1 AS NUMERIC) AS total_trades,
    TOTAL_FEES_USD AS total_fees_usd,
    PLATFORM_FEE_USD AS total_platform_fees,
    CREATOR_FEE_USD AS total_creator_fees
FROM 
    ETHEREUM_FLIPSIDE.NFT.EZ_NFT_SALES
WHERE 
    PLATFORM_NAME ILIKE 'magic eden%' 
    AND TOTAL_FEES_USD != 0
    AND DATE(BLOCK_TIMESTAMP) >= '2024-2-6'
ORDER BY 
    DATE(BLOCK_TIMESTAMP) DESC, tx_hash