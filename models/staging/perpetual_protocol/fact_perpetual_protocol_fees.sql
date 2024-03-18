{{ config(materialized="table") }}

with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_perpetual_protocol_fees") }}
    ),

    data as (
        select parse_json(source_json) as data
        from {{ source("PROD_LANDING", "raw_perpetual_protocol_fees") }}
        where extraction_date = (select max_date from max_extraction)
    ),
    data_per_chain as (
        select
            lower(value:chain::string) chain,
            to_date(regexp_substr(value:day::string, '^(.*)U', 1, 1, 'e', 1)) as date,
            lower(value:protocol::string) as app,
            value:fees_generated::double as fees,
            'DeFi' as category
        from data, lateral flatten(input => data)
    )
select chain, date, 'perpetual_protocol' as app, fees, 'DeFi' as category
from data_per_chain
union all
select
    null as chain,
    date,
    'perpetual_protocol' as app,
    sum(fees) as fees,
    'DeFi' as category
from data_per_chain
group by date, app
