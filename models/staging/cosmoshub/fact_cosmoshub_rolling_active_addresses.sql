{{ config(materialized="view") }}
with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_cosmoshub_rolling_active_addresses") }}
    )
select
    date(value:date) as date,
    value:"wau"::int as wau,
    value:"mau"::int as mau,
    'cosmoshub' as chain
from
    {{ source("PROD_LANDING", "raw_cosmoshub_rolling_active_addresses") }},
    lateral flatten(input => parse_json(source_json))
where extraction_date = (select max_date from max_extraction) and date(value:date) is not null
