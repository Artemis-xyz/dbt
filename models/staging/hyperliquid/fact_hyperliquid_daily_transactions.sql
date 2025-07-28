with
    latest_source_json as (
        select extraction_date, source_url, source_json
        from {{ source("PROD_LANDING", "raw_hyperliquid_daily_transactions") }}
        where extraction_date = (select max(extraction_date) from {{ source("PROD_LANDING", "raw_hyperliquid_daily_transactions") }})
    ),

    extracted_daily_transactions as (
        select
            value:daily_trades::NUMBER as trades,
            'hyperliquid' as app,
            'hyperliquid' as chain,
            'DeFi' as category,
            date(value:time) as date
        from latest_source_json, lateral flatten(input => parse_json(source_json))
    )
select
    trades,
    date,
    'hyperliquid' as app,
    'hyperliquid' as chain,
    'DeFi' as category
from extracted_daily_transactions
