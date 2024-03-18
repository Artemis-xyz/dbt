with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_dydx_v4_unique_traders") }}
    ),
    latest_data as (
        select parse_json(source_json) as data
        from {{ source("PROD_LANDING", "raw_dydx_v4_unique_traders") }}
        where extraction_date = (select max_date from max_extraction)
    ),
    flattened_data as (
        select
            date(to_timestamp(value:date::number / 1000)) as date,
            value:"unique_senders"::int as unique_traders
        from latest_data, lateral flatten(input => data)
    )
select date, unique_traders, 'dydx_v4' as app, 'DeFi' as category, 'dydx_v4' as chain
from flattened_data
order by date desc
