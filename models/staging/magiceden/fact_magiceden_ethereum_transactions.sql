{{
    config(
        materialized="table",
        snowflake_warehouse="MAGICEDEN"
    )
}}

SELECT 
    DATE(block_timestamp) AS date,
    'ethereum' AS chain,
    SUM(price_usd) AS trading_volume,
    COUNT(DISTINCT seller_address) + COUNT(DISTINCT buyer_address) AS active_wallets,
    COUNT(*) AS total_trades,
    SUM(total_fees_usd) AS total_fees_usd,
    SUM(platform_fee_usd) AS total_platform_fees,
    SUM(creator_fee_usd) AS total_creator_fees
FROM
    ethereum_flipside.nft.ez_nft_sales
WHERE 
    platform_name ilike 'magic eden%'
    AND total_fees_usd != 0
    AND DATE(block_timestamp) >= '2024-02-06'
GROUP BY 
    DATE(block_timestamp)
ORDER BY 
    DATE(block_timestamp) DESC