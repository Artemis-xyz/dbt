{{ config(materialized="table") }}
with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_fantom_txns") }}
    )
select
    date(to_timestamp(value:date::number / 1000)) as date,
    value:txns::int as txns,
    value as source,
    'fantom' as chain
from
    {{ source("PROD_LANDING", "raw_fantom_txns") }},
    lateral flatten(input => parse_json(source_json))
where extraction_date = (select max_date from max_extraction)
