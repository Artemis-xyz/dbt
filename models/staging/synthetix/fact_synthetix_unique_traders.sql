with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_synthetix_unique_traders") }}
    ),
    data as (
        select parse_json(source_json) data
        from {{ source("PROD_LANDING", "raw_synthetix_unique_traders") }}
        where extraction_date = (select max_date from max_extraction)
    )
select
    value:"DAU"::double as unique_traders,
    to_date(regexp_substr(value:day, '^(.*)U', 1, 1, 'e', 1)) as date,
    'synthetix' as app,
    'DeFi' as category,
    'optimism' as chain
from data, lateral flatten(input => data)
