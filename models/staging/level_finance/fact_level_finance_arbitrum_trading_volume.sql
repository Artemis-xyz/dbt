with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_level_finance_arbitrum_trading_volume") }}
    ),
    data as (
        select parse_json(source_json) data
        from {{ source("PROD_LANDING", "raw_level_finance_arbitrum_trading_volume") }}
        where extraction_date = (select max_date from max_extraction)
    )
select
    'arbitrum' chain,
    to_date(regexp_substr(value:time::string, '^(.*)U', 1, 1, 'e', 1)) as date,
    'level_finance' as app,
    value:leverage::double as trading_volume,
    'DeFi' as category
from data, lateral flatten(input => data)
