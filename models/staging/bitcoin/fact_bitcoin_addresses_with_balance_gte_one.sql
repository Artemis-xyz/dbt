{{ config(materialized="table") }}
with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_bitcoin_addresses_with_balance_gte_one") }}
    )
select
    date(to_timestamp(value:date::number / 1000)) as date,
    value:addresses::int as addresses,
    value as source,
    'bitcoin' as chain
from
    {{ source("PROD_LANDING", "raw_bitcoin_addresses_with_balance_gte_one") }},
    lateral flatten(input => parse_json(source_json))
where extraction_date = (select max_date from max_extraction)
