{{ config(materialized="view") }}
with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_cosmoshub_txns") }}
    )
select
    date(value:date) as date,
    value:txns::int as txns,
    value as source,
    'cosmoshub' as chain
from
    {{ source("PROD_LANDING", "raw_cosmoshub_txns") }},
    lateral flatten(input => parse_json(source_json))
where extraction_date = (select max_date from max_extraction)
