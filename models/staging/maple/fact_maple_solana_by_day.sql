{{ 
    config(
        materialized="table",
        snowflake_warehouse="MAPLE",
    )
}}

with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_maple_solana_by_day") }}
    )
select
    value:parsed_date::date as date,
    value:timestamp::int as timestamp,
    value:pool_name::string as pool_name,
    value:outstanding_usd::number as outstanding_usd
from
    {{ source("PROD_LANDING", "raw_maple_solana_by_day") }},
    lateral flatten(input => parse_json(source_json))
where extraction_date = (select max_date from max_extraction)
