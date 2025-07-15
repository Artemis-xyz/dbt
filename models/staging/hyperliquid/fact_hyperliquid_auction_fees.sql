{{ config(materialized="table", snowflake_warehouse="HYPERLIQUID") }}

with
    latest_source_json as (
        select extraction_date, source_url, source_json
        from {{ source("PROD_LANDING", "raw_hyperliquid_auction_fees") }}
        order by extraction_date desc
        limit 1
    ),

    extracted_fees as (
        select
            value:name::string as name,
            value:deployGas::numeric as auction_fees,
            'hyperliquid' as app,
            'hyperliquid' as chain,
            'DeFi' as category,
            date(value:time) as date
        from latest_source_json, lateral flatten(input => parse_json(source_json))
    )
select
    name,
    auction_fees,
    date,
    'hyperliquid' as app,
    'hyperliquid' as chain,
    'DeFi' as category
from extracted_fees
