with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_ton_revenue") }}
    ),
    latest_data as (
        select parse_json(source_json) as data
        from {{ source("PROD_LANDING", "raw_ton_revenue") }}
        where extraction_date = (select max_date from max_extraction)
    ),
    flattened_data as (
        select value:"timestamp"::date as date, value:"value"::float as revenue_native
        from latest_data, lateral flatten(input => data)
    ),
    prices as ({{ get_coingecko_price_with_latest("the-open-network") }})
select flattened_data.date, revenue_native, revenue_native * price as revenue, 'ton' as chain
from flattened_data
left join prices using(date)