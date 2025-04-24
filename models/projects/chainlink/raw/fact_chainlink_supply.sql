{{
    config(
        materialized="table",
        snowflake_warehouse="CHAINLINK",
        database="chainlink",
        schema="raw",
        alias="fact_chainlink_supply",
    )
}}

with max_extraction as (
    select max(extraction_date) as max_date
    from {{ source('PROD_LANDING', 'raw_chainlink_supply') }}
)
select
    left(value:date::string, 10) as date,
    value:premine_unlocks_native::float as premine_unlocks_native,
    value:circulating_supply_native::float as circulating_supply_native
from
    {{ source('PROD_LANDING', 'raw_chainlink_supply') }},
    lateral flatten(input => parse_json(source_json))
where extraction_date = (select max_date from max_extraction)