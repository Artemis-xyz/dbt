{{
    config(
        materialized="table",
        snowflake_warehouse="GAINS_NETWORK",
     )
}}


with max_extraction as (
    select max(extraction_date) as max_date
    from {{ source("PROD_LANDING", "raw_gains_trading_volume_unique_traders_v8_v9") }}
)
select
    value:date as date,
    value:blockchain as chain,
    'gains_network' as app,
    'DeFi' as category,
    value:trading_volume as trading_volume,
    value:unique_traders as unique_traders,
    value:count_trades as count_trades
from {{ source("PROD_LANDING", "raw_gains_trading_volume_unique_traders_v8_v9") }}
, lateral flatten(input => parse_json(source_json))
where extraction_date = (select max_date from max_extraction)
