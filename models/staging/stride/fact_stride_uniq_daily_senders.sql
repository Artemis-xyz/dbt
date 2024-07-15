{{
    config(
        materialized="view",
    )
}}

with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_stride_uniq_daily_senders" ) }}
    ),
    stride_data as (
        select parse_json(source_json) as data
        from {{ source("PROD_LANDING", "raw_stride_uniq_daily_senders" ) }}
        where extraction_date = (select max_date from max_extraction)
    ),
    uniqs as (
        select
            to_timestamp(value:date::number / 1000)::date as date,
            value:sender sender,
            'stride' as chain
        from stride_data, lateral flatten(input => data)
    )
select date, sender, chain
from uniqs