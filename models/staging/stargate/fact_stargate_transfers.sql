{{ config(materialized="table") }}

with raw_data as (
    select 
        source
        , src_timestamp
        , src_hash
        , dst_timestamp
        , dst_hash
        , src_chain
        , dst_chain
        , src_token_address
        , usd_value
        , treasury_fee
        , depositor
        , recipient
    from {{ source("PROD_LANDING", "raw_stargate_data_dump") }}
)

select
    source as version
    , src_timestamp
    , src_hash
    , src_chain
    , depositor
    , src_token_address
    , coalesce(dst_timestamp, src_timestamp) as dst_timestamp
    , dst_chain
    , recipient
    , usd_value
    , treasury_fee
from raw_data
