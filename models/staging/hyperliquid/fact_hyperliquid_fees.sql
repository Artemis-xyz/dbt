WITH latest_source_json AS (
    SELECT extraction_date, source_url, source_json
    FROM {{ source("PROD_LANDING", "raw_hyperliquid_fees") }}
    ORDER BY extraction_date DESC
    LIMIT 1
),

extracted_fees AS (
    SELECT
        value:total_fees AS fees,
        value:total_spot_fees AS spot_fees,
        'hyperliquid' AS app,
        'hyperliquid' AS chain,
        'DeFi' AS category,
        value:time AS timestamp
    FROM latest_source_json, LATERAL FLATTEN(input => PARSE_JSON(source_json))
),

max_fees AS (
    SELECT
        chain,
        DATE(timestamp) AS fee_date,
        MAX_BY(fees, timestamp) / 1e6 AS max_trading_fees,
        MAX_BY(spot_fees, timestamp) / 1e6 AS max_spot_fees
    FROM extracted_fees
    GROUP BY chain, DATE(timestamp)
)

SELECT
    max_trading_fees,
    max_spot_fees,
    max_trading_fees - COALESCE(LAG(max_trading_fees) OVER (PARTITION BY chain ORDER BY fee_date ASC), 0) AS trading_fees,
    max_spot_fees - COALESCE(LAG(max_spot_fees) OVER (PARTITION BY chain ORDER BY fee_date ASC), 0) AS spot_fees,
    (trading_fees - spot_fees) AS perp_fees,
    fee_date AS timestamp,
    'hyperliquid' AS app,
    'hyperliquid' AS chain,
    'DeFi' AS category
FROM max_fees