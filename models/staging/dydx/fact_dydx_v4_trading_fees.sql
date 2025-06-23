with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_dydx_v4_trading_fees") }}
    ),
    latest_data as (
        select parse_json(source_json) as data
        from {{ source("PROD_LANDING", "raw_dydx_v4_trading_fees") }}
        where extraction_date = (select max_date from max_extraction)
    ),
    flattened_data as (
        select
            parse_json(data):labels::timestamp_ntz as date,
            parse_json(data):total_fees::float as total_fees,
            'dydx_v4' as app,
            'DeFi' as category,
            'dydx_v4' as chain
        from latest_data
    ),
    daily_data as (
        select
            date(date) as date,
            sum(total_fees) as total_fees,
            app,
            category,
            chain
        from flattened_data
        group by date(date), app, category, chain
    )
select date, total_fees, app, category, chain
from daily_data
where date < (select max(date) from daily_data)
order by date desc
