{{ config(materialized="view") }}
with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_fantom_uniq_daily_addresses" ) }}
    ),
    fantom_data as (
        select parse_json(source_json) as data
        from {{ source("PROD_LANDING", "raw_fantom_uniq_daily_addresses" ) }}
        where extraction_date = (select max_date from max_extraction)
    ),
    uniqs as (
        select
            to_timestamp(value:date::number / 1000)::date as date,
            value:from_address from_address,
            'fantom' as chain
        from fantom_data, lateral flatten(input => data)
    )
select date, from_address, chain
from uniqs