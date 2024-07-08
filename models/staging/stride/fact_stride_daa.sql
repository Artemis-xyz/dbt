{{
    config(
        materialized="view",
        snowflake_warehouse="STRIDE",
    )
}}

with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_stride_daa") }}
    ),
    latest_data as (
        select parse_json(source_json) as data
        from {{ source("PROD_LANDING", "raw_stride_daa") }}
        where extraction_date = (select max_date from max_extraction)
    ),
    flattened_data as (
        select to_date(value:date::string) as date, value:"daa"::float as daa
        from latest_data, lateral flatten(input => data)
    )
select date, daa, 'stride' as chain
from flattened_data
