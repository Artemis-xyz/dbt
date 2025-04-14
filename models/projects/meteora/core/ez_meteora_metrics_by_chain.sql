
{{
    config(
        materialized='table',
        snowflake_warehouse='MEDIUM',
        database='METEORA',
        schema='core',
        alias='ez_metrics_by_chain',
    )
 }}

with date_spine as (
    select date_spine.date
    from {{ ref('dim_date_spine') }} date_spine
    where date_spine.date between '2023-10-01' and to_date(sysdate())
)

, swap_metrics as (
    select *
    from {{ ref('fact_meteora_swap_metrics') }}
)
, api_metrics as (
    select
        date,
        'solana' as chain,
        coalesce(daily_fee, 0) as daily_fee,
        coalesce(daily_trade_volume, 0) as daily_trade_volume,
        coalesce(total_fee, 0) as total_fee,
        coalesce(total_trade_volume, 0) as total_trade_volume,
        coalesce(total_tvl, 0) as total_tvl
    from {{ ref('fact_meteora_api_metrics') }}
)

select
    date_spine.date,
    'solana' as chain,

     --Standardized Metrics

    --Financial Metrics
    coalesce(api_metrics.daily_fee, 0) as spot_dlmm_fees,
    coalesce(api_metrics.daily_fee, 0) as gross_protocol_revenue,
    coalesce(api_metrics.daily_fee, 0) * .05 as treasury_cash_flow,
    coalesce(api_metrics.daily_fee, 0) * .95 as protocol_cash_flow,
    coalesce(api_metrics.total_tvl, 0) as dlmm_tvl,
    --Usage Metrics 
    coalesce(swap_metrics.unique_traders, 0) as spot_dlmm_dau,
    coalesce(swap_metrics.number_of_swaps, 0) as spot_dlmm_txns,
    coalesce(swap_metrics.trading_volume, 0) as spot_dlmm_volume,

from date_spine
left join swap_metrics using(date)
left join api_metrics using(date)
where date_spine.date < to_date(sysdate())
