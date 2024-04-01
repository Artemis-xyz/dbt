{{ config(materialized="table") }}
with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_rocketpool_staking_minipool_count") }}
    ),
    latest_data as (
        select extraction_date::date as date, parse_json(source_json) as data
        from {{ source("PROD_LANDING", "raw_rocketpool_staking_minipool_count") }}
        where extraction_date = (select max_date from max_extraction)
    ),
    flattened_data as (
        select
            date as extraction_date,
            flattened.value:"date"::string as date,
            flattened.value:"value"::float as value
        from latest_data, lateral flatten(input => data) as flattened
    )

select date, value
from flattened_data
order by date desc
