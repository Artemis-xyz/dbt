WITH latest_source_json AS (
    SELECT extraction_date, source_url, source_json
    FROM {{ source("PROD_LANDING", "raw_hyperliquid_fees") }}
    where extraction_date = (select max(extraction_date) from {{ source("PROD_LANDING", "raw_hyperliquid_fees") }})
),

extracted_fees AS (
    SELECT
        value:total_fees::number AS total_fees,
        value:total_spot_fees::number AS total_spot_fees,
        value:time AS timestamp
    FROM latest_source_json, LATERAL FLATTEN(input => PARSE_JSON(source_json))
),

max_fees AS (
    SELECT
        timestamp,
        total_fees / 1e6 AS max_trading_fees,
        total_spot_fees / 1e6 AS max_spot_fees
    FROM extracted_fees
)
, fee_data as (
    SELECT
        timestamp,
        max_trading_fees,
        max_spot_fees,
        max_trading_fees - COALESCE(LAG(max_trading_fees) OVER (ORDER BY timestamp ASC), 0) AS trading_fees,
        max_spot_fees - COALESCE(LAG(max_spot_fees) OVER (ORDER BY timestamp ASC), 0) AS spot_fees,
        (trading_fees - spot_fees) AS perp_fees
    FROM max_fees
)
select
    date(timestamp)::date as date,
    'hyperliquid' AS app,
    'hyperliquid' AS chain,
    'DeFi' AS category,
    sum(trading_fees) as trading_fees,
    sum(spot_fees) as spot_fees,
    sum(perp_fees) as perp_fees
from fee_data
group by 1