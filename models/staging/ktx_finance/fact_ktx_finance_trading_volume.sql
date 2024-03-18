with
    latest_source_json as (
        select extraction_date, source_url, source_json
        from {{ source("PROD_LANDING", "raw_ktx_finance_trading_volume") }}
        order by extraction_date desc
        limit 1
    )

select
    to_date(value:id::string) as date,
    value:margin::float / 1e30 as trading_volume,
    'ktx_finance' as app,
    value:chain::string as chain,
    'DeFi' as category
from latest_source_json, lateral flatten(input => parse_json(source_json))
