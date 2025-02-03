with
    latest_source_json as (
        select extraction_date, source_url, source_json
        from {{ source("PROD_LANDING", "raw_hyperliquid_fees") }}
        order by extraction_date desc
        limit 1
    ),

    extracted_fees as (
        select
            value:total_fees as fees,
            value:total_spot_fees as spot_fees,
            'hyperliquid' as app,
            'hyperliquid' as chain,
            'DeFi' as category,
            value:time as timestamp
        from latest_source_json, lateral flatten(input => parse_json(source_json))
        where date(timestamp) >= '2024-12-22' 
    )
select
    max_by(fees, timestamp) / 1e6 AS max_trading_fees,
    max_by(spot_fees, timestamp) / 1e6 AS max_spot_fees,
    CASE 
        WHEN date(timestamp) >= '2024-12-23' THEN 
            max_by(fees, timestamp) - COALESCE(LAG(max_by(fees, timestamp)) OVER (PARTITION BY chain ORDER BY timestamp ASC), 0)
        ELSE NULL
    END AS trading_fees,
    CASE 
        WHEN date(timestamp) >= '2024-12-23' THEN 
            max_by(spot_fees, timestamp) - COALESCE(LAG(max_by(spot_fees, timestamp)) OVER (PARTITION BY chain ORDER BY timestamp ASC), 0)
        ELSE NULL
    END AS spot_fees,
    (max_by(fees, timestamp) - max_by(spot_fees, timestamp)) AS perp_fees,
    timestamp,
    'hyperliquid' as app,
    'hyperliquid' as chain,
    'DeFi' as category
from extracted_fees
group by chain, timestamp

