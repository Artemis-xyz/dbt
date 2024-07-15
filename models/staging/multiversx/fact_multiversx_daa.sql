{{ config(materialized="view") }}
with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_multiversx_daa") }}
    ),
    multiversx_data as (
        select parse_json(source_json) as data
        from {{ source("PROD_LANDING", "raw_multiversx_daa") }}
        where extraction_date = (select max_date from max_extraction)
    )
select
    date(value:time) as date,
    coalesce(value:value, 0) as daa,
    value as source,
    'multiversx' as chain
from multiversx_data, lateral flatten(input => data:data[0]:"all")
