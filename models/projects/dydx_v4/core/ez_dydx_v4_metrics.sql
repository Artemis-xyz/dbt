{{
    config(
        materialized="incremental",
        snowflake_warehouse="DYDX",
        database="dydx_v4",
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

with
    trading_volume_data as (
        select date, trading_volume
        from {{ ref("fact_dydx_v4_trading_volume") }}
    )
    , fees_data as (
        select date, maker_fees, taker_fees, fees
        from {{ ref("fact_dydx_v4_fees") }}
    )
    , chain_data as (
        select date, maker_fees, maker_rebates, txn_fees
        from {{ ref("fact_dydx_v4_txn_fees") }}
    )
    , trading_fees as (
        select date, total_fees
        from {{ ref("fact_dydx_v4_trading_fees") }}
    )
    , unique_traders_data as (
        select date, unique_traders
        from {{ ref("fact_dydx_v4_unique_traders") }}
    )

select 
    unique_traders_data.date as date
    , 'dydx_v4' as app
    , 'DeFi' as category
    , trading_volume
    , unique_traders
    , fees_data.maker_fees
    , taker_fees
    , fees
    , fees_data.maker_fees + fees_data.taker_fees as trading_fees -- Trading fees is maker_fees+taker_fees
    , txn_fees -- chain transaction fees (not really significant)
    -- standardize metrics
    , trading_volume as perp_volume
    , unique_traders as perp_dau
    , trading_fees + txn_fees as ecosystem_revenue
    , case when unique_traders_data.date > '2025-03-25' then ecosystem_revenue * 0.25 else 0 end as buybacks
    -- timestamp columns
    , sysdate() as created_on
    , sysdate() as modified_on
from trading_volume_data
left join fees_data on trading_volume_data.date = fees_data.date
left join chain_data on trading_volume_data.date = chain_data.date
left join unique_traders_data on trading_volume_data.date = unique_traders_data.date
where true 
{{ ez_metrics_incremental('unique_traders_data.date', backfill_date) }}
and unique_traders_data.date < to_date(sysdate())
