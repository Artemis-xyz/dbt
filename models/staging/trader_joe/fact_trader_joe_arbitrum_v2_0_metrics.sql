{{
    config(
        materialized = 'table',
        snowflake_warehouse = 'trader_joe'
    )
}}

with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{source('PROD_LANDING', 'raw_trader_joe_arbitrum_v2_0_metrics')}}
        
    ),
    latest_data as (
        select parse_json(source_json) as data
        from {{source('PROD_LANDING', 'raw_trader_joe_arbitrum_v2_0_metrics')}}
        where extraction_date = (select max_date from max_extraction)
    )
select
    f.value:date::date as date,
    'arbitrum' as chain,
    'v2.0' as version,
    f.value:"protocolFeesUsd"::int as protocol_fees,
    f.value:"feesUsd"::int as total_fees,
    f.value:"volumeUsd"::int as total_volume
from latest_data, lateral flatten(input => data) f
