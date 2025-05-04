{{
    config(
        materialized="incremental",
        snowflake_warehouse="MAGICEDEN",
        unique_key=["tx_id"]
    )
}}

-- a very small portion of magic eden v1 tx_ids contained multiple nft sales in the fact_nft_sales table
WITH transaction_data AS (
    -- Extract and combine transaction details
    SELECT
        DATE(t.block_timestamp) AS date,
        t.tx_id,
        n.price,
        n.seller_address AS seller,
        n.buyer_address AS purchaser,
        m.nft_collection_name,
        -- Extract platform and maker fees
        COALESCE(
            CAST(REGEXP_SUBSTR(ARRAY_TO_STRING(t.log_messages, ','), '"maker_fee":([0-9]+)', 1, 1, 'e', 1) AS BIGINT) / 1e9,
            0
        ) AS maker_fee_sol,
        COALESCE(
            CAST(REGEXP_SUBSTR(ARRAY_TO_STRING(t.log_messages, ','), '"taker_fee":([0-9]+)', 1, 1, 'e', 1) AS BIGINT) / 1e9,
            CAST(REGEXP_SUBSTR(ARRAY_TO_STRING(t.log_messages, ','), '"lp_fee":([0-9]+)', 1, 1, 'e', 1) AS BIGINT) / 1e9,
            0
        ) AS platform_fee_sol,
        -- Extract royalties and total price
        COALESCE(
            CAST(REGEXP_SUBSTR(ARRAY_TO_STRING(t.log_messages, ','), '"royalty":([0-9]+)', 1, 1, 'e', 1) AS BIGINT) / 1e9,
            CAST(REGEXP_SUBSTR(ARRAY_TO_STRING(t.log_messages, ','), '"royalty_paid":([0-9]+)', 1, 1, 'e', 1) AS BIGINT) / 1e9,
            0
        ) AS royalty_sol,
        COALESCE(
            CAST(REGEXP_SUBSTR(ARRAY_TO_STRING(t.log_messages, ','), '"total_price":([0-9]+)', 1, 1, 'e', 1) AS BIGINT) / 1e9,
            CAST(REGEXP_SUBSTR(ARRAY_TO_STRING(t.log_messages, ','), '"price":([0-9]+)', 1, 1, 'e', 1) AS BIGINT) / 1e9,
            0
        ) AS price_sol
    FROM 
        solana_flipside.core.fact_transactions t
    JOIN 
        solana_flipside.nft.ez_nft_sales n
    ON 
        t.tx_id = n.tx_id
    LEFT JOIN 
        solana_flipside.nft.dim_nft_metadata m
    ON 
        n.mint = m.mint
    WHERE 
        n.marketplace ILIKE 'magic eden%' -- Filter for Magic Eden marketplace
        AND n.succeeded = TRUE -- Ensure the transaction succeeded
        {% if is_incremental() %}
            AND t.block_timestamp >= (SELECT DATEADD(DAY, -3, MAX(date)) FROM {{ this }})
        {% else %}
            AND DATE(n.block_timestamp) >= '2020-03-16'
        {% endif %}

),
converted_data AS (
    -- Join transactions with SOL to USD rates and calculate USD values
    SELECT
        td.date,
        td.tx_id,
        td.price,
        td.seller,
        td.purchaser,
        td.nft_collection_name,
        td.maker_fee_sol,
        td.platform_fee_sol,
        td.royalty_sol,
        td.price_sol,
        sr.price,
        td.price * sr.price AS sales_amount_usd,
        td.maker_fee_sol * sr.price AS maker_fee_usd,
        td.platform_fee_sol * sr.price AS platform_fee_usd,
        td.royalty_sol * sr.price AS royalty_usd,
        td.price_sol * sr.price AS price_usd
    FROM 
        transaction_data td
    LEFT JOIN 
         solana_flipside.price.ez_prices_hourly sr
    ON 
        DATE_TRUNC('HOUR', td.date) = sr.HOUR
    WHERE 
        sr.IS_NATIVE = TRUE
),
aggregated_data AS (
    -- Aggregate metrics
    SELECT
        date,
        tx_id,
        SUM(sales_amount_usd) AS daily_trading_volume_usd,
        COUNT(DISTINCT seller) + COUNT(DISTINCT purchaser) AS active_wallets,
        COUNT(DISTINCT nft_collection_name) AS collections_transacted,
        COUNT(tx_id) AS total_trades,
        SUM(platform_fee_usd + maker_fee_usd) AS total_platform_fees,
        SUM(royalty_usd) AS total_creator_fees,
        SUM(platform_fee_usd + maker_fee_usd + royalty_usd) AS total_fees_usd
    FROM 
        converted_data
    GROUP BY 
        date, tx_id
)
SELECT 
    date,
    tx_id,
    'solana' as chain,
    daily_trading_volume_usd as trading_volume,
    active_wallets,
    collections_transacted,
    total_trades,
    total_platform_fees,
    total_creator_fees,
    total_fees_usd
FROM 
    aggregated_data
ORDER BY 
    date DESC

