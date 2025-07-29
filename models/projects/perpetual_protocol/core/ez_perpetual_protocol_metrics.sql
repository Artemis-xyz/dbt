{{
    config(
        materialized="incremental",
        snowflake_warehouse="PERPETUAL_PROTOCOL",
        database="perpetual_protocol",
        schema="core",
        alias="ez_metrics",
        incremental_strategy="merge",
        unique_key="date",
        on_schema_change="append_new_columns",
        merge_update_columns=var("backfill_columns", []),
        merge_exclude_columns=["created_on"] if not var("backfill_columns", []) else none,
        full_refresh=var("full_refresh", false),
        tags=["ez_metrics"],
    )
}}

{% set backfill_date = var("backfill_date", None) %}

WITH 
    perp_data as (
        SELECT
            date
            , sum(perp_volume) as perp_volume
            , sum(perp_dau) as perp_dau
            , sum(fees) as fees
            , sum(revenue) as revenue
            , sum(tvl) as tvl
            -- standardize metrics
            , sum(perp_volume) as perp_volume
            , sum(perp_dau) as perp_dau
            , sum(fees) as fees
            , sum(tvl_pct_change) as tvl_pct_change
            , sum(treasury_fee_allocation) as treasury_fee_allocation
            , sum(staking_fee_allocation) as staking_fee_allocation
            , sum(treasury_fee_allocation) as treasury_fee_allocation
            , sum(service_fee_allocation) as service_fee_allocation
        FROM {{ ref("ez_perpetual_protocol_metrics_by_chain") }}
        WHERE date < to_date(sysdate())
        GROUP BY 1
    )
    , market_data as ({{ get_coingecko_metrics("perpetual-protocol") }})

    , token_incentives as (
        select
            date,
            SUM(total_token_incentives) as token_incentives
        from {{ref('fact_perpetual_token_incentives')}}
        group by 1
    )

SELECT
    date
    , 'perpetual-protocol' as artemis_id

    -- Standardized Metrics
    , perp_data.perp_dau
    , perp_data.perp_volume
    , perp_data.tvl
    , perp_data.tvl_pct_change

    -- Fees Metrics
    , perp_data.fees as perp_fees
    , perp_data.fees
    , perp_data.staking_fee_allocation
    , perp_data.service_fee_allocation
    , perp_data.treasury_fee_allocation
    
    -- Financial Metrics
    , perp_data.revenue
    , coalesce(token_incentives.token_incentives, 0) as token_incentives
    , coalesce(perp_data.revenue, 0) - coalesce(token_incentives.token_incentives, 0) as earnings

    -- Market Data
    , price
    , market_cap
    , fdmc
    , token_turnover_circulating
    , token_turnover_fdv
    , token_volume

    -- timestamp columns
    , to_timestamp_ntz(current_timestamp()) as created_on
    , to_timestamp_ntz(current_timestamp()) as modified_on
FROM perp_data
LEFT JOIN market_data USING(date)
LEFT JOIN token_incentives USING(date)
WHERE true
{{ ez_metrics_incremental('date', backfill_date) }}
AND date < to_date(sysdate())
