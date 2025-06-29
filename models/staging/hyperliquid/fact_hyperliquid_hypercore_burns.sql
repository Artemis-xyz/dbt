with
    latest_source_json as (
        select extraction_date, source_url, source_json
        from {{ source("PROD_LANDING", "raw_hyperliquid_daily_burn") }}
        order by extraction_date desc
        limit 1
    ),

    extracted_daily_burn as (
        select
            key::date as date, 
            -- HyperCore Spot Token Fees Burned
            value::double as daily_burn,
            'hyperliquid' as app,
            'hyperliquid' as chain,
            'DeFi' as category
        from latest_source_json, lateral flatten(input => parse_json(source_json:daily_burned))
    )
select
    date,
    daily_burn as hypercore_burns_native,
    app,
    chain,
    category
from extracted_daily_burn
