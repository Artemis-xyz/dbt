{{
    config(
        materialized="table",
        snowflake_warehouse="ARBITRUM",
    )
}}

WITH decoded_table AS (
    SELECT
        DATE(block_timestamp) AS date, 
        block_number, 
        block_timestamp, 
        tx_hash, 
        decoded_log:firstPriceAmount::NUMBER AS rawFirstPriceAmount, 
        decoded_log:firstPriceAmount::NUMBER / 1e18 AS firstPriceAmount, 
        decoded_log:isMultiBidAuction::BOOLEAN AS isMultiBidAuction, 
        decoded_log:price::NUMBER / 1e18 AS price, 
        decoded_log:firstPriceAmount::NUMBER / 1e18, 
        decoded_log,
    FROM {{ source('ARBITRUM_FLIPSIDE', 'ez_decoded_event_logs') }}
    WHERE LOWER(contract_address) = LOWER('0x5fcb496a31b7AE91e7c9078Ec662bd7A55cd3079')
        AND event_name = 'AuctionResolved'
    ORDER BY DATE(block_timestamp) DESC
)

SELECT
    dt.date, 
    SUM(
        CASE
            WHEN dt.isMultiBidAuction THEN dt.price
            ELSE dt.firstPriceAmount
        END
    ) AS timeboost_fees, 
    SUM(
        CASE
            WHEN dt.isMultiBidAuction THEN dt.price * eph.price
            ELSE dt.firstPriceAmount * eph.price
        END
    ) AS timeboost_fees_usd
FROM decoded_table AS dt
JOIN {{ source('ARBITRUM_FLIPSIDE', 'ez_prices_hourly') }} AS eph
    ON DATE_TRUNC('hour', dt.block_timestamp) = eph.hour AND LOWER(eph.token_address) = LOWER('0x82aF49447D8a07e3bd95BD0d56f35241523fBab1')
GROUP BY 1
ORDER BY 1 DESC