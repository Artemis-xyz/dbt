{{ config(materialized="view") }}
with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_multiversx_gas_fees") }}
    ),
    multiversx_data as (
        select parse_json(source_json) as data
        from {{ source("PROD_LANDING", "raw_multiversx_gas_fees") }}
        where extraction_date = (select max_date from max_extraction)
    ),
    price_data as ({{ get_coingecko_metrics("elrond-erd-2") }})
select
    date(to_timestamp(data:timestamp::number)) as date,
    data:value::float / pow(10, 18) as gas,
    gas * price as gas_usd,
    'multiversx' as chain
from multiversx_data
left join price_data on date(to_timestamp(data:timestamp::number)) = price_data.date
order by date desc
