with
    latest_source_json as (
        select extraction_date, source_url, source_json
        from {{ source("PROD_LANDING", "raw_hyperliquid_unique_traders") }}
        order by extraction_date desc
        limit 1
    ),

    extracted_unique_traders as (
        select
            value:daily_unique_users::number as unique_traders,
            'hyperliquid' as app,
            'hyperliquid' as chain,
            'DeFi' as category,
            date(value:time) as date
        from latest_source_json, lateral flatten(input => parse_json(source_json))
    )
select
    unique_traders,
    date,
    'hyperliquid' as app,
    'hyperliquid' as chain,
    'DeFi' as category
from extracted_unique_traders
