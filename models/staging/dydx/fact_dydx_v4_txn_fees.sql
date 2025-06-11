with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_dydx_v4_txn_fees") }}
    ),
    latest_data as (
        select parse_json(source_json) as data
        from {{ source("PROD_LANDING", "raw_dydx_v4_txn_fees") }}
        where extraction_date = (select max_date from max_extraction)
    ),
    flattened_data as (
        select
            parse_json(data):labels::timestamp_ntz as date,
            parse_json(data):maker_fees::float as maker_fees,
            parse_json(data):maker_rebates::float as maker_rebates,
            parse_json(data):txn_fees::float as txn_fees,
            'dydx_v4' as app,
            'DeFi' as category,
            'dydx_v4' as chain
        from latest_data
    ),
    daily_data as (
        select
            date(date) as date,
            sum(maker_fees) as maker_fees,
            sum(maker_rebates) as maker_rebates,
            sum(txn_fees) as txn_fees,
            app,
            category,
            chain
        from flattened_data
        group by date(date), app, category, chain
    )
select date, maker_fees, maker_rebates, txn_fees, app, category, chain
from daily_data
where date < (select max(date) from daily_data)
order by date desc
