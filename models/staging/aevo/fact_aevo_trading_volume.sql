with
    latest_source_json as (
        select extraction_date, source_url, source_json
        from {{ source("PROD_LANDING", "raw_aevo_trading_volume") }}
        order by extraction_date desc
        limit 1
    ),

    aevo_volume as (
        select
            to_date(value:date::string) as date,
            value:volume::float as trading_volume,
            value:type::string as type,
            'aevo' as app,
            'aevo' as chain,
            'DeFi' as category
        from latest_source_json, lateral flatten(input => parse_json(source_json))
        where type = 'PERPETUAL'
    )

select trading_volume, date, 'aevo' as app, 'aevo' as chain, 'DeFi' as category
from aevo_volume
