{{ config(materialized="view") }}
with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_cosmoshub_daa") }}
    )
select
    date(value:date) as date,
    value:"daa"::int as daa,
    'cosmoshub' as chain
from
    {{ source("PROD_LANDING", "raw_cosmoshub_daa") }},
    lateral flatten(input => parse_json(source_json))
where extraction_date = (select max_date from max_extraction) and date(value:date) is not null
