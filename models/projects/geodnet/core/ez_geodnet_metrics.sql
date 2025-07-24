{{
    config(
        materialized="incremental",
        snowflake_warehouse="GEODNET",
        database="geodnet",
        schema="core",
        alias="ez_metrics",
        incremental_strategy="merge",
        unique_key="date",
        on_schema_change="append_new_columns",
        merge_update_columns=var("backfill_columns", []),
        merge_exclude_columns=["created_on"] | reject('in', var("backfill_columns", [])) | list,
        full_refresh=false,
        tags=["ez_metrics"],
    )
}}

{% set backfill_date = var("backfill_date", None) %}

with
    revenue_data as (
        select date, fees, revenue, protocol
        from {{ ref("fact_geodnet_fees_revenue") }}
    ),
    price_data as ({{ get_coingecko_metrics("geodnet") }})
select
    revenue_data.date
    , coalesce(fees, 0) as fees
    , coalesce(revenue, 0) as revenue
    -- Standardized Metrics
    -- Token Metrics
    , coalesce(price, 0) as price
    , coalesce(market_cap, 0) as market_cap
    , coalesce(fdmc, 0) as fdmc
    , coalesce(token_volume, 0) as token_volume
    -- Cash Flow Metrics
    , coalesce(revenue, 0) as ecosystem_revenue
    , coalesce(revenue, 0) * 0.8 as buyback_fee_allocation
    , coalesce(revenue, 0) * 0.2 as foundation_fee_allocation
    -- Turnover Metrics
    , coalesce(token_turnover_circulating, 0) as token_turnover_circulating
    , coalesce(token_turnover_fdv, 0) as token_turnover_fdv
    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on
from revenue_data
left join price_data on revenue_data.date = price_data.date
where true
{{ ez_metrics_incremental('revenue_data.date', backfill_date) }}
and revenue_data.date < to_date(sysdate())
