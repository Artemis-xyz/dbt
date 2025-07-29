with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_holdstation_bera_trading_volume") }}
    ),
    holdstation_volume as (
        select parse_json(source_json) as data
        from {{ source("PROD_LANDING", "raw_holdstation_bera_trading_volume") }}
        where extraction_date = (select max_date from max_extraction)
    )

select
    to_date(value:date::string) as date,
    value:volume::double as perp_volume,
    'holdstation' as app,
    'berachain' as chain
from holdstation_volume, lateral flatten(input => data)
