{{
    config(
        materialized="incremental",
        snowflake_warehouse="CENTRIFUGE",
        database="centrifuge",
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
    fundamental_data as (
        select
            date, 
            txns,
            daa, 
            fees_native, 
            fees_usd
        from {{ ref("fact_centrifuge_fundamental_metrics") }}
    )
    , market_data as (
        {{ get_coingecko_metrics("centrifuge") }}
    )
select
    date
    , txns
    , daa as dau
    , coalesce(fees_native, 0) as fees_native
    , coalesce(fees_usd, 0) as fees
    -- Standardized Metrics
    -- Token Metrics
    , coalesce(market_data.price, 0) as price
    , coalesce(market_data.market_cap, 0) as market_cap
    , coalesce(market_data.fdmc, 0) as fdmc
    , coalesce(market_data.token_volume, 0) as token_volume 
    -- Chain Metrics
    , daa as chain_dau
    , txns as chain_txns
    , coalesce(fees_native, 0) as l1_fee_allocation_native
    , coalesce(fees_usd, 0) as l1_fee_allocation
    , coalesce(fees_usd, 0)/coalesce(txns, 1) as chain_avg_txn_fee
    -- Cash Flow Metrics
    , coalesce(fees_usd, 0) as ecosystem_revenue
    , coalesce(fees_native, 0) as ecosystem_revenue_native
    -- Turnover Metrics
    , coalesce(market_data.token_turnover_circulating, 0) as token_turnover_circulating
    , coalesce(market_data.token_turnover_fdv, 0) as token_turnover_fdv
    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on
from fundamental_data
left join market_data using (date)
where true
{{ ez_metrics_incremental('fundamental_data.date', backfill_date) }}
and fundamental_data.date < to_date(sysdate())
