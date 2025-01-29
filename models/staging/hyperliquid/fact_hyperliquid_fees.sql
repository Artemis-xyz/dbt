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
    fees,
    spot_fees,
    timestamp,
    'hyperliquid' as app,
    'hyperliquid' as chain,
    'DeFi' as category
from extracted_fees
