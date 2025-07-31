{{
    config(
        materialized='incremental',
        snowflake_warehouse='BALANCER',
        database='BALANCER',
        schema='core',
        alias='ez_metrics',
        incremental_strategy='merge',
        unique_key='date',
        on_schema_change='append_new_columns',
        merge_update_columns=var("backfill_columns", []),
        merge_exclude_columns=["created_on"] if not var("backfill_columns", []) else none,
        full_refresh=false,
        tags=["ez_metrics"],
    )
}}

{% set backfill_date = var("backfill_date", None) %}

with date_spine as (
    select date
    from {{ ref('dim_date_spine') }}
    where date between '2020-03-01' and to_date(sysdate())
)
, swap_metrics as (
    SELECT
        block_timestamp::date as date,
        count(distinct sender) as unique_traders,
        count(*) as number_of_swaps,
        sum(trading_volume) as trading_volume,
        sum(fee_usd) as trading_fees,
        sum(service_cash_flow) as primary_supply_side_revenue,
        sum(treasury_cash_flow + vebal_cash_flow) as revenue,
        sum(service_cash_flow) as service_fee_allocation,
        sum(treasury_cash_flow) as treasury_fee_allocation,
        sum(vebal_cash_flow) as vebal_fee_allocation
    FROM {{ ref('ez_balancer_dex_swaps') }}
    group by 1
)
, token_incentives as (
    SELECT
        date,
        sum(amount_usd) as token_incentives_usd
    FROM {{ ref('fact_balancer_token_incentives_all_chains') }}
    group by 1
)
, all_tvl as (
    SELECT
        date,
        sum(amount_usd) as tvl_usd
    FROM {{ ref('fact_balancer_tvl_by_chain_and_token') }}
    group by 1
)
, treasury as (
    SELECT
        date,
        sum(usd_balance) as net_treasury_usd
    FROM {{ ref('fact_balancer_treasury_by_token') }}
    group by 1
)
, treasury_native as (
    SELECT
        date,
        sum(native_balance) as treasury_native,
        sum(usd_balance) as own_token_treasury
    FROM {{ ref('fact_balancer_treasury_by_token') }}
    WHERE token = 'BAL'
    group by 1
)
, net_treasury as (
    SELECT
        date,
        sum(usd_balance) as net_treasury_usd
    FROM {{ ref('fact_balancer_treasury_by_token') }}
    WHERE token <> 'BAL'
    group by 1
)
, token_holders as (
    SELECT
        date,
        token_holder_count
    FROM {{ ref('fact_balancer_token_holders') }}
)
, market_data as (
    {{ get_coingecko_metrics('balancer') }}
)
select
    date_spine.date
    , 'balancer' as artemis_id

    --Market Data
    , market_data.price as price
    , market_data.market_cap as mc
    , market_data.fdmc as fdmc
    , market_data.token_volume as token_volume

    --Usage Data
    , swap_metrics.unique_traders as spot_dau
    , swap_metrics.unique_traders as dau
    , swap_metrics.number_of_swaps as spot_txns
    , swap_metrics.number_of_swaps as txns
    , all_tvl.tvl_usd as tvl
    , swap_metrics.trading_volume as spot_volume

    --Fee Data
    , swap_metrics.trading_fees / price as fees_native
    , swap_metrics.trading_fees as spot_fees
    , swap_metrics.trading_fees as fees

    --Fee Allocation
    , swap_metrics.service_fee_allocation as lp_fee_allocation
    , swap_metrics.treasury_fee_allocation as foundation_fee_allocation
    , swap_metrics.vebal_fee_allocation as staking_fee_allocation

    --Financial Statements
    , swap_metrics.revenue / price as revenue_native
    , swap_metrics.revenue as revenue
    , coalesce(token_incentives.token_incentives_usd, 0) as token_incentives
    , coalesce(swap_metrics.revenue, 0) - coalesce(token_incentives.token_incentives_usd, 0) as earnings

    --Treasury Data
    , treasury.net_treasury_usd as treasury
    , net_treasury.net_treasury_usd as net_treasury
    , treasury_native.own_token_treasury as own_token_treasury

    --Token Turnover/Other Data
    , market_data.token_turnover_circulating as token_turnover_circulating
    , market_data.token_turnover_fdv as token_turnover_fdv
    , token_holders.token_holder_count as tokenholder_count

    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on
from date_spine
left join all_tvl using (date)
left join treasury using (date)
left join treasury_native using (date)
left join net_treasury using (date)
left join token_holders using (date)
left join market_data using (date)
left join swap_metrics using (date)
left join token_incentives using (date)
where true
{{ ez_metrics_incremental('date_spine.date', backfill_date) }}
and date_spine.date < to_date(sysdate())