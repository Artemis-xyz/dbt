{{
    config(
        materialized="table",
        snowflake_warehouse="MAGICEDEN"
    )
}}

SELECT 
    DATE(BLOCK_TIMESTAMP) AS date,
    'ethereum' as chain,
    SUM(PRICE_USD) AS daily_trading_volume_usd,
    COUNT(DISTINCT SELLER_ADDRESS) + COUNT(DISTINCT BUYER_ADDRESS) AS active_wallets,
    COUNT(DISTINCT PROJECT_NAME) AS collections_transacted,
    COUNT(*) AS total_trades,
    SUM(TOTAL_FEES_USD) AS total_fees_usd,
    SUM(PLATFORM_FEE_USD) AS total_platform_fees,
    SUM(CREATOR_FEE_USD) AS total_creator_fees
FROM 
    ETHEREUM_FLIPSIDE.NFT.EZ_NFT_SALES
WHERE 
    PLATFORM_NAME ILIKE 'magic eden%' AND TOTAL_FEES_USD != 0
    AND DATE(BLOCK_TIMESTAMP) >= '2024-2-6'
GROUP BY 
    1
ORDER BY
    1 DESC