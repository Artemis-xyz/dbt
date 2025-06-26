{{ config(materialized="table") }}

with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_perpetual_protocol_unique_traders") }}
    ),
    data as (
        select parse_json(source_json) data
        from {{ source("PROD_LANDING", "raw_perpetual_protocol_unique_traders") }}
        where extraction_date = (select max_date from max_extraction)
    )
select
    case
        when lower(value:chain::string) = 'all'
        then null
        else lower(value:chain::string)
    end as chain,
    to_date(regexp_substr(value:day::string, '^(.*)U', 1, 1, 'e', 1)) as date,
    'perpetual_protocol' as app,
    value:unique_traders::number as unique_traders,
    'DeFi' as category
from data, lateral flatten(input => data)
