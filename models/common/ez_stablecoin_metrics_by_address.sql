-- depends_on: {{ ref('agg_daily_stablecoin_breakdown_silver') }}

{{ 
    config(
        snowflake_warehouse="COMMON",
        database="common",
        schema="core",
        materialized='view'
    )
}}

select 
    date
    , from_address
    , contract_name
    , contract
    , application
    , icon
    , app
    , category
    , is_wallet

    , contract_address
    , symbol

    , stablecoin_transfer_volume
    , stablecoin_daily_txns
    , artemis_stablecoin_transfer_volume
    , artemis_stablecoin_daily_txns
    , p2p_stablecoin_transfer_volume
    , p2p_stablecoin_daily_txns
    , stablecoin_supply
    , chain
    , unique_id
from {{ ref('agg_daily_stablecoin_breakdown_silver') }}
