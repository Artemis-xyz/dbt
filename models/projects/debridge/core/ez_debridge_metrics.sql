{{
    config(
        materialized="incremental",
        snowflake_warehouse="DEBRIDGE",
        database="debridge",
        schema="core",
        alias="ez_metrics",
        incremental_strategy="merge",
        unique_key="date",
        on_schema_change="append_new_columns",
        merge_update_columns=var("backfill_columns", []),
        merge_exclude_columns=["created_on"] if not var("backfill_columns", []) else none,
        full_refresh=false,
        tags=["ez_metrics"],
    )
}}

{% set backfill_date = var("backfill_date", None) %}

with bridge_volume_fees as (
    select 
        date
        , bridge_volume
        , ecosystem_revenue
        , bridge_txns
        , bridge_txns as txns
        , bridge_dau
        , ecosystem_revenue as fees
    from {{ ref("fact_debridge_fundamental_metrics") }}
)

, price_data as ({{ get_coingecko_metrics("debridge") }})

select
    bridge_volume_fees.date
    , ecosystem_revenue
    , txns
    , fees
    -- Standardized Metrics
    , bridge_volume
    , bridge_dau
    , bridge_txns
    , price_data.price as price
    , price_data.market_cap as market_cap
    , price_data.fdmc as fdmc
    , price_data.token_turnover_circulating as token_turnover_circulating
    , price_data.token_turnover_fdv as token_turnover_fdv
    , price_data.token_volume as token_volume
    -- timestamp columns
    , sysdate() as created_on
    , sysdate() as modified_on
from bridge_volume_fees
left join price_data on bridge_volume_fees.date = price_data.date
where true
{{ ez_metrics_incremental('bridge_volume_fees.date', backfill_date) }}
and bridge_volume_fees.date < to_date(sysdate())
