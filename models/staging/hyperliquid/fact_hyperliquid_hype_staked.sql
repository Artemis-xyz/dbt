{{ config(materialized="table", snowflake_warehouse="HYPERLIQUID") }}

with
    latest_source_json as (
        select extraction_date, source_url, source_json
        from {{ source("PROD_LANDING", "raw_hyperliquid_hype_staked") }}
        order by extraction_date desc
        limit 1
    )
    , extracted_hype_staked as (
        select
            value:snapshot_timestamp::int as snapshot_timestamp
            , value:num_holders::int as num_holders
            , value:total_staked::float as total_staked
            , 'hyperliquid' as app
            , 'hyperliquid' as chain
            , 'DeFi' as category
        from latest_source_json, lateral flatten(input => parse_json(source_json))
)
select
    date(snapshot_timestamp) as date
    , num_holders as num_stakers
    ,total_staked as staked_hype
    , app
    , chain
    , category
from extracted_hype_staked