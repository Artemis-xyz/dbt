{{ config(materialized="view") }}
with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_multiversx_txns") }}
    ),
    multiversx_data as (
        select parse_json(source_json) as data
        from {{ source("PROD_LANDING", "raw_multiversx_txns") }}
        where extraction_date = (select max_date from max_extraction)
    )
select
    date(to_timestamp(data:timestamp::number)) as date,
    data:value::int as txns,
    'multiversx' as chain
from multiversx_data
order by date desc
