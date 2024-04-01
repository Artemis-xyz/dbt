with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_holdstation_unique_traders") }}
    ),
    holdstation_unique_traders as (
        select parse_json(source_json) as data
        from {{ source("PROD_LANDING", "raw_holdstation_unique_traders") }}
        where extraction_date = (select max_date from max_extraction)
    )

select
    to_date(value:date::string) as date,
    value:existing_traders::integer + value:new_traders::integer as unique_traders,
    'holdstation' as app,
    'zksync' as chain,
    'DeFi' as category
from holdstation_unique_traders, lateral flatten(input => data)
