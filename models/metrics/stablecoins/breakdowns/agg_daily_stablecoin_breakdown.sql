{{ config(materialized="table") }}
select
    date
    , contract_address
    , symbol
    , from_address
    , contract_name
    , contract
    , application
    , icon
    , app
    , category
    , stablecoin_transfer_volume
    , stablecoin_daily_txns
    , stablecoin_dau
    , p2p_stablecoin_transfer_volume
    , p2p_stablecoin_daily_txns
    , p2p_stablecoin_dau
    , stablecoin_supply
    , chain
from {{ ref("agg_daily_stablecoin_breakdown_silver") }}