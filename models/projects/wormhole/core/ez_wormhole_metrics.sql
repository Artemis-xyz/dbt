{{ config(
    materialized="table",
    warehouse="WORMHOLE",
    database="WORMHOLE",
    schema="core",
    alias="ez_metrics"
) }}

with txns_data as (
    select
        date,
        txns
    from {{ ref("fact_wormhole_txns") }}
)
, daa as (
    select
        date,
        bridge_daa
    from {{ ref("fact_wormhole_bridge_daa_gold") }}
)
, bridge_volume as (
    select date, sum(bridge_volume) as bridge_volume, sum(fees) as fees
    from {{ ref("fact_wormhole_bridge_volume_gold") }}
    group by 1
)
, supply_data as (
    select
        date
        , premine_unlocks_native
        , net_supply_change_native
        , circulating_supply_native
    from {{ ref("fact_wormhole_supply_data") }}
)
, price_data as ({{ get_coingecko_metrics("wormhole") }})

select
    coalesce(txns_data.date, daa.date) as date
    , coalesce(daa.bridge_daa, 0) as bridge_daa
    , coalesce(bridge_volume.fees, 0) as fees

    -- Standardized Metrics
    , coalesce(txns_data.txns, 0) as bridge_txns
    , coalesce(daa.bridge_daa, 0) as bridge_dau
    , coalesce(bridge_volume.bridge_volume, 0) as bridge_volume
    , coalesce(bridge_volume.fees, 0) as bridge_fees
    , coalesce(bridge_volume.fees, 0) as ecosystem_revenue
    , price_data.price as price
    , price_data.market_cap as market_cap
    , price_data.fdmc as fdmc
    , price_data.token_turnover_circulating as token_turnover_circulating
    , price_data.token_turnover_fdv as token_turnover_fdv
    , price_data.token_volume as token_volume

    -- Supply Data
    , premine_unlocks_native
    , net_supply_change_native
    , circulating_supply_native
from txns_data
left join daa on txns_data.date = daa.date
left join bridge_volume on txns_data.date = bridge_volume.date
left join price_data on txns_data.date = price_data.date
left join supply_data on txns_data.date = supply_data.date
where coalesce(txns_data.date, daa.date) < to_date(sysdate())
