{{ config(materialized="table", cluster_by=['chain']) }}
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
    , artemis_stablecoin_transfer_volume
    , artemis_stablecoin_daily_txns
    , p2p_stablecoin_transfer_volume
    , p2p_stablecoin_daily_txns
    , stablecoin_supply
    , is_wallet
    , chain
from {{ ref("agg_daily_stablecoin_breakdown_silver") }}