{{
    config(
        materialized='incremental',
        snowflake_warehouse='CONVEX',
        database='CONVEX',
        schema='core',
        alias='ez_metrics',
        incremental_strategy='merge',
        unique_key='date',
        on_schema_change='append_new_columns',
        merge_update_columns=var("backfill_columns", []),
        merge_exclude_columns=["created_on"] if not var("backfill_columns", []) else none,
        full_refresh=var("full_refresh", false),
        tags=["ez_metrics"],
    )
}}

{% set backfill_date = var("backfill_date", None) %}

with date_spine as (
    select date
    from {{ ref('dim_date_spine') }}
    where date between '2020-03-01' and to_date(sysdate())
)
, fees_and_revenue as (
    select
        date
        , coalesce(sum(fees), 0) as fees
        , coalesce(sum(revenue), 0) as revenue
        , coalesce(sum(primary_supply_side_fees), 0) as primary_supply_side_fees
    from {{ ref('fact_convex_revenue') }}
    group by 1
)
, token_incentives as (
    select
        date
        , coalesce(sum(token_incentives), 0) as token_incentives
    from {{ ref('fact_convex_token_incentives') }}
    group by 1
)
, tvl as (
    select
        date
        , coalesce(sum(tvl), 0) as tvl
    from {{ ref('fact_convex_combined_tvl') }}
    group by 1
)
, treasury as (
    select 
        date
        , sum(treasury) as treasury
        , sum(treasury_native) as treasury_native
        , sum(net_treasury) as net_treasury
        , sum(net_treasury_native) as net_treasury_native
        , sum(own_token_treasury) as own_token_treasury
        , sum(own_token_treasury_native) as own_token_treasury_native
    from {{ ref('ez_convex_metrics_by_token') }}
    group by 1
)
, token_holders as (
    SELECT
        date
        , coalesce(token_holder_count, 0) as token_holder_count
    FROM {{ ref('fact_convex_token_holders') }}
)
, market_metrics as (
    {{ get_coingecko_metrics('convex-finance') }}
)

select
    date_spine.date
    , 'convex' as artemis_id

    -- Standardized Metrics

    -- Market Data
    , market_metrics.price
    , market_metrics.market_cap
    , market_metrics.fdmc
    , market_metrics.token_volume

    -- Usage Data
    , tvl.tvl
    , tvl.tvl - lag(tvl.tvl) over (order by date) as tvl_net_change
    , tvl.tvl as net_deposits
    , treasury.treasury as treasury_value
    , token_holders.token_holder_count

    -- Fee Data
    , fees_and_revenue.fees
    , (fees_and_revenue.primary_supply_side_fees + 0.005 * (fees_and_revenue.revenue + fees_and_revenue.primary_supply_side_fees)) as service_fee_allocation
    , (0.145 * (fees_and_revenue.revenue + fees_and_revenue.primary_supply_side_fees)) as staking_fee_allocation
    , (0.02 * (fees_and_revenue.revenue + fees_and_revenue.primary_supply_side_fees)) as treasury_fee_allocation
    
    -- Financial Statements
    , fees_and_revenue.revenue
    , token_incentives.token_incentives
    , token_incentives.token_incentives as expenses
    , fees_and_revenue.revenue - token_incentives.token_incentives as earnings
    
    -- Treasury Data
    , treasury.treasury
    , treasury.treasury_native
    , treasury.net_treasury
    , treasury.net_treasury_native
    , treasury.own_token_treasury
    , treasury.own_token_treasury_native

    -- Turnover Metrics
    , market_metrics.token_turnover_circulating
    , market_metrics.token_turnover_fdv

    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on

from date_spine
left join treasury using (date)
left join token_holders using (date)
left join fees_and_revenue using (date)
left join token_incentives using (date)
left join tvl using (date)
left join market_metrics using (date)
where true
{{ ez_metrics_incremental('date_spine.date', backfill_date) }}
and date_spine.date < to_date(sysdate())