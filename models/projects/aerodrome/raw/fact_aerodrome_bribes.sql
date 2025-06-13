{{
    config(
        materialized='table',
        snowflake_warehouse='AERODROME',
        database='aerodrome',
        schema='raw',
        alias='fact_aerodrome_bribes'
    )
}}

with max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_aerodrome_bribes") }}
)

select
    value:date::date as date,
    value:rewardToken_address::string as contract_address,
    value:bribes::float as bribes
from
    {{ source("PROD_LANDING", "raw_aerodrome_bribes") }},
    lateral flatten(input => parse_json(source_json))
where extraction_date = (select max_date from max_extraction)


