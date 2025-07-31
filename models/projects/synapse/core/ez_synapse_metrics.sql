{{
    config(
        materialized="incremental",
        snowflake_warehouse="SYNAPSE",
        database="synapse",
        schema="core",
        alias="ez_metrics",
        incremental_strategy="merge",
        unique_key="date",
        on_schema_change="append_new_columns",
        merge_update_columns=var("backfill_columns", []),
        merge_exclude_columns=["created_on"] if not var("backfill_columns", []) else none,
        full_refresh=var("full_refresh", false),
        tags=["ez_metrics"]
    )
}}

{% set backfill_date = var("backfill_date", None) %}

with
    bridge_volume_metrics as (
        select date, coalesce(bridge_volume, 0) as bridge_volume
        from {{ ref("fact_synapse_bridge_volume") }}
        where chain is null
    ),
    bridge_daa_metrics as (
        select date, coalesce(bridge_daa, 0) as bridge_daa
        from {{ ref("fact_synapse_bridge_daa") }}
    )
    , market_metrics as ({{ get_coingecko_metrics("synapse-2") }})
select
    bridge_daa_metrics.date as date
    , 'synapse' as artemis_id
    
    -- Standardized Metrics

    -- Market Data
    , market_metrics.price as price
    , market_metrics.market_cap as market_ca
    , market_metrics.fdmc as fdmc
    , market_metrics.token_volume as token_volume

    -- Usage Data
    , bridge_daa_metrics.bridge_daa as bridge_dau
    , bridge_volume_metrics.bridge_volume as bridge_volume

    -- Token Turnover/Other Data    
    , market_metrics.token_turnover_circulating as token_turnover_circulating
    , market_metrics.token_turnover_fdv as token_turnover_fdv

    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on

from bridge_volume_metrics
left join bridge_daa_metrics on bridge_volume_metrics.date = bridge_daa_metrics.date
left join price_data on bridge_volume_metrics.date = price_data.date
where true
{{ ez_metrics_incremental('bridge_daa_metrics.date', backfill_date) }}
and bridge_daa_metrics.date < to_date(sysdate())
