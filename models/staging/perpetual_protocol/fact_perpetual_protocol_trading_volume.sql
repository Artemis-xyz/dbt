{{ config(materialized="table") }}

with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_perpetual_protocol_trading_volume") }}
    ),
    data as (
        select parse_json(source_json) data
        from {{ source("PROD_LANDING", "raw_perpetual_protocol_trading_volume") }}
        where extraction_date = (select max_date from max_extraction)
    ),
    data_per_chain as (
        select
            lower(value:chain::string) chain,
            to_date(regexp_substr(value:day::string, '^(.*)U', 1, 1, 'e', 1)) as date,
            lower(value:protocol::string) as app,
            value:trading_volume::double as trading_volume,
            'DeFi' as category
        from data, lateral flatten(input => data)
    )
select chain, date, 'perpetual_protocol' as app, trading_volume, 'DeFi' as category
from data_per_chain
union all
select
    null as chain,
    date,
    'perpetual_protocol' as app,
    sum(trading_volume) as trading_volume,
    'DeFi' as category
from data_per_chain
group by date, app
