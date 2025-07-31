{{ config(
    materialized="incremental",
    warehouse="WORMHOLE",
    database="WORMHOLE",
    schema="core",
    alias="ez_metrics",
    incremental_strategy="merge",
    unique_key="date",
    on_schema_change="append_new_columns",
    merge_update_columns=var("backfill_columns", []),
    merge_exclude_columns=["created_on"] if not var("backfill_columns", []) else none,
    full_refresh=var("full_refresh", false),
    tags=["ez_metrics"]
) }}

{% set backfill_date = var("backfill_date", None) %}

with txns_data as (
    select
        date
        , coalesce(txns, 0) as txns
    from {{ ref("fact_wormhole_txns") }}
)
, daa as (
    select
        date
        , coalesce(bridge_daa, 0) as bridge_daa
    from {{ ref("fact_wormhole_bridge_daa_gold") }}
)
, bridge_volume as (
    select date, sum(coalesce(bridge_volume, 0)) as bridge_volume, sum(coalesce(fees, 0)) as fees
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
, market_metrics as ({{ get_coingecko_metrics("wormhole") }})

select
    coalesce(txns_data.date, daa.date) as date
    , 'wormhole' as artemis_id
    
    -- Standardized Metrics

    -- Market Data
    , market_metrics.price as price
    , market_metrics.market_cap as market_cap
    , market_metrics.fdmc as fdmc
    , market_metrics.token_volume as token_volume

    -- Usage Data
    , daa.bridge_daa as bridge_dau
    , daa.bridge_daa as dau
    , txns_data.txns as bridge_txns
    , txns_data.txns as txns
    , bridge_volume.bridge_volume as bridge_volume
    , bridge_volume.fees as bridge_fees

    -- Supply Data
    , supply_data.premine_unlocks_native
    , supply_data.net_supply_change_native
    , supply_data.circulating_supply_native

    -- Token Turnover/Other Metrics
    , market_metrics.token_turnover_circulating as token_turnover_circulating
    , market_metrics.token_turnover_fdv as token_turnover_fdv

    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on

from txns_data
left join daa on txns_data.date = daa.date
left join bridge_volume on txns_data.date = bridge_volume.date
left join price_data on txns_data.date = price_data.date
left join supply_data on txns_data.date = supply_data.date
where true
{{ ez_metrics_incremental('txns_data.date', backfill_date) }}
and coalesce(txns_data.date, daa.date) < to_date(sysdate())
