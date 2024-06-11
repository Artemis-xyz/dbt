{{ config(snowflake_warehouse="STAKEWISE", materialized="table") }}
with
    seth2_max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_seth2_count") }}
    ),
    seth2_latest_data as (
        select extraction_date::date as date, parse_json(source_json) as data
        from {{ source("PROD_LANDING", "raw_seth2_count") }}
        where extraction_date = (select max_date from seth2_max_extraction)
    ),
    seth2_flattened_data as (
        select
            date as extraction_date,
            flattened.value:"date"::string as date,
            flattened.value:"value"::float as value
        from seth2_latest_data, lateral flatten(input => data) as flattened
    ),
    reth2_max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_reth2_count") }}
    ),
    reth2_latest_data as (
        select extraction_date::date as date, parse_json(source_json) as data
        from {{ source("PROD_LANDING", "raw_reth2_count") }}
        where extraction_date = (select max_date from reth2_max_extraction)
    ),
    reth2_flattened_data as (
        select
            date as extraction_date,
            flattened.value:"date"::string as date,
            flattened.value:"value"::float as value
        from reth2_latest_data, lateral flatten(input => data) as flattened
    )

select s.date, s.value as seth2_value, r.value as reth2_value
from seth2_flattened_data s
join reth2_flattened_data r on s.date = r.date
order by date desc
