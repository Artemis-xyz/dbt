with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_dydx_v4_fees") }}
    ),
    latest_data as (
        select parse_json(source_json) as data
        from {{ source("PROD_LANDING", "raw_dydx_v4_fees") }}
        where extraction_date = (select max_date from max_extraction)
    ),
    flattened_data as (
        select
            date(to_timestamp(value:order_date::number / 1000)) as date,
            value:"total_taker_order_fees"::float as taker_fees,
            value:"total_maker_order_fees"::float as maker_fees,
            value:"trade_fees"::float as fees
        from latest_data, lateral flatten(input => data)
    )
select
    date,
    taker_fees,
    maker_fees,
    fees,
    'dydx_v4' as app,
    'DeFi' as category,
    'dydx_v4' as chain
from flattened_data
order by date desc
