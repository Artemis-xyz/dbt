{{
    config( 
        materialized="incremental",
        snowflake_warehouse="RIPPLE",
        database="ripple",
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
    fundamental_data as (
        select
            date,
            chain_fees,
            chain_fees_native,
            chain_dau,
            chain_txns
        from {{ ref("fact_ripple_fundamental_metrics") }}
    )
    , price_data as ({{ get_coingecko_metrics("ripple") }})
    , supply_data as (
        select
            date,
            issued_supply,
            circulating_supply
        from {{ ref("fact_ripple_supply_data") }}
    )
select
    fundamental_data.date -- goes back to Jan 2013

    -- Old metrics for compatibility
    , chain_dau as dau
    , chain_txns as txns

    -- Market Metrics
    , price_data.price
    , price_data.market_cap
    , price_data.fdmc
    , price_data.token_volume

    -- Usage Metrics
    , chain_dau
    , chain_txns
    
    -- Cash Flow Metrics
    , chain_fees
    , chain_fees as fees
    , chain_fees as revenue
    , chain_fees as burned_fee_allocation
    , chain_fees_native as fees_native
    , chain_fees_native as revenue_native
    , chain_fees_native as burned_fee_allocation_native

    -- Supply Metrics
    , issued_supply as issued_supply_native
    , circulating_supply as circulating_supply_native

    -- Other Metrics
    , price_data.token_turnover_circulating
    , price_data.token_turnover_fdv

    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on
FROM fundamental_data
left join price_data using(date)
left join supply_data using(date)
where true
{{ ez_metrics_incremental('fundamental_data.date', backfill_date) }}
and fundamental_data.date < to_date(sysdate())