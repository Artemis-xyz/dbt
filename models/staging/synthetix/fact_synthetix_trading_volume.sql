with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_synthetix_trading_volume") }}
    ),
    data as (
        select parse_json(source_json) data
        from {{ source("PROD_LANDING", "raw_synthetix_trading_volume") }}
        where extraction_date = (select max_date from max_extraction)
    )
select
    value:"total_volume"::double as trading_volume,
    value:time::date as date,
    'synthetix' as app,
    'DeFi' as category,
    'optimism' as chain
from data, lateral flatten(input => data)
