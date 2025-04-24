{{ config(materialized="table") }}

with max_extraction as (
    select max(extraction_date) as max_date
    from {{ source("PROD_LANDING", "raw_helium_gross_emissions") }}
),
latest_data as (
    select extraction_date::date as date, parse_json(source_json) as data
        from {{ source("PROD_LANDING", "raw_helium_gross_emissions") }}
        where extraction_date = (select max_date from max_extraction)
    ),
    flattened_data as (
        select
            flattened.value:"block_date"::string as date,
            flattened.value:"holders"::float as emissions_native_to_holders,
            flattened.value:"treasury"::float as emissions_native_to_treasury,
            flattened.value:"total"::float as emissions_native_to_total
        from latest_data, lateral flatten(input => data) as flattened
    )

select 
    date,
    coalesce(emissions_native_to_holders, 0) as emissions_native_to_holders,
    coalesce(emissions_native_to_treasury, 0) as emissions_native_to_treasury,
    coalesce(emissions_native_to_total, 0) as emissions_native_to_total
from flattened_data
where date < to_date(sysdate())
order by date desc