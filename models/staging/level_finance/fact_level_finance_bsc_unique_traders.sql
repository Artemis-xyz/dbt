with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_level_finance_bsc_unique_traders") }}
    ),
    data as (
        select parse_json(source_json) data
        from {{ source("PROD_LANDING", "raw_level_finance_bsc_unique_traders") }}
        where extraction_date = (select max_date from max_extraction)
    )
select
    'bsc' chain,
    to_date(regexp_substr(value:time::string, '^(.*)U', 1, 1, 'e', 1)) as date,
    'level_finance' as app,
    value:num_traders::double as unique_traders,
    'DeFi' as category
from data, lateral flatten(input => data)
