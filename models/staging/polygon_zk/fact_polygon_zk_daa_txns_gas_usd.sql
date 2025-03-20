{{ config(materialized="view") }}
with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_polygon_zk_data") }}
    ),
    polygon_zk_data as (
        select parse_json(source_json) as data
        from {{ source("PROD_LANDING", "raw_polygon_zk_data") }}
        where extraction_date = (select max_date from max_extraction)
    ), 
    prices as ({{ get_coingecko_price_with_latest('ethereum') }})
    , extracted_data as (
        select
            date(left(value:"date_time"::string, 10)) as date,
            value:unique_active_users::number as daa,
            value:all_transactions::number as txns,
            value:txn_fees::double as gas,
            value as source,
            'polygon_zk' as chain
        from polygon_zk_data, 
        lateral flatten(input => data:"data":"records")
        where date < to_date(sysdate())
    )
select 
    extracted_data.date,
    daa,
    txns,
    gas,
    gas * price as gas_usd,
    'polygon_zk' as chain
from extracted_data
left join prices using(date)


