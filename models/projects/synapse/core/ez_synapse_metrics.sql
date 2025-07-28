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
        full_refresh=false,
        tags=["ez_metrics"]
    )
}}

{% set backfill_date = var("backfill_date", None) %}

with
    bridge_volume_metrics as (
        select date, bridge_volume
        from {{ ref("fact_synapse_bridge_volume") }}
        where chain is null
    ),
    bridge_daa_metrics as (
        select date, bridge_daa
        from {{ ref("fact_synapse_bridge_daa") }}
    )
    , price_data as ({{ get_coingecko_metrics("synapse-2") }})
select
    bridge_daa_metrics.date as date,
    'synapse' as app,
    'Bridge' as category,
    bridge_daa_metrics.bridge_daa
    -- Standardized Metrics
    , bridge_volume_metrics.bridge_volume
    , bridge_daa_metrics.bridge_daa as bridge_dau
    , price_data.price as price
    , price_data.market_cap as market_cap
    , price_data.fdmc as fdmc
    , price_data.token_turnover_circulating as token_turnover_circulating
    , price_data.token_turnover_fdv as token_turnover_fdv
    , price_data.token_volume as token_volume
    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on
from bridge_volume_metrics
left join bridge_daa_metrics on bridge_volume_metrics.date = bridge_daa_metrics.date
left join price_data on bridge_volume_metrics.date = price_data.date
where true
{{ ez_metrics_incremental('bridge_daa_metrics.date', backfill_date) }}
and bridge_daa_metrics.date < to_date(sysdate())
