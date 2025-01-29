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
            date(value:time) as date_col
        from latest_source_json, lateral flatten(input => parse_json(source_json))
        where date(value:time) >= '2024-12-23' 
    )
select
    fees,
    spot_fees,
    date_col,
    'hyperliquid' as app,
    'hyperliquid' as chain,
    'DeFi' as category
from extracted_fees
