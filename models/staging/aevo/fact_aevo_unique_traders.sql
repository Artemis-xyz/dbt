with
    latest_source_json as (
        select extraction_date, source_url, source_json
        from {{ source("PROD_LANDING", "raw_aevo_unique_traders") }}
        order by extraction_date desc
        limit 1
    ),

    extracted_unique_traders as (
        select
            value:dau::number as unique_traders,
            'aevo' as app,
            'aevo' as chain,
            'DeFi' as category,
            to_date(value:date::string) as date
        from latest_source_json, lateral flatten(input => parse_json(source_json))
    )
select unique_traders, date, 'aevo' as app, 'aevo' as chain, 'DeFi' as category
from extracted_unique_traders
