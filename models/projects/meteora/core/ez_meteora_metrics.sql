
{{
    config(
        materialized='table',
        snowflake_warehouse='METEORA',
        database='METEORA',
        schema='core',
        alias='ez_metrics',
    )
 }}

with dlmm_metrics as (
    select
        date,
        dlmm_spot_fees,
        tvl,
        unique_traders,
        number_of_swaps,
        trading_volume
    from {{ref('fact_meteora_dlmm_metrics')}}
)
, amm_metrics as (
    select
        date,
        fees,
        unique_traders,
        number_of_swaps,
        trading_volume,
    from {{ ref('fact_meteora_amm_metrics') }}
)
, spot_metrics as (
    select
        date,
        unique_traders,
        number_of_swaps,
        trading_volume
    from {{ ref('fact_meteora_spot_swap_metrics') }}
)
, date_spine as (
    select date_spine.date
    from {{ ref('dim_date_spine') }} date_spine
    where date_spine.date between '2023-10-01' and to_date(sysdate())
)

select
    date_spine.date

    --Old Metrics needed for backwards compatibility
    , coalesce(dlmm_metrics.unique_traders, 0) as unique_traders
    , coalesce(dlmm_metrics.number_of_swaps, 0) as number_of_swaps
    , coalesce(dlmm_metrics.trading_volume, 0) as trading_volume

    -- Standardized Metrics

    -- Usage Metrics
    , coalesce(dlmm_metrics.unique_traders, 0) as dlmm_spot_dau
    , coalesce(amm_metrics.unique_traders, 0) as amm_spot_dau
    , coalesce(spot_metrics.unique_traders, 0) as spot_dau
    , coalesce(dlmm_metrics.number_of_swaps, 0) as dlmm_spot_txns
    , coalesce(amm_metrics.number_of_swaps, 0) as amm_spot_txns
    , coalesce(spot_metrics.number_of_swaps, 0) as spot_txns
    , coalesce(dlmm_metrics.trading_volume, 0) as dlmm_spot_volume
    , coalesce(amm_metrics.trading_volume, 0) as amm_spot_volume
    , coalesce(dlmm_metrics.trading_volume, 0) + coalesce(amm_metrics.trading_volume, 0) as spot_volume
    , coalesce(dlmm_metrics.tvl, 0) as dlmm_tvl
    , coalesce(dlmm_metrics.tvl, 0) as tvl
    
    -- Cash Flow Metrics
    , coalesce(amm_metrics.fees, 0) as amm_spot_fees
    , coalesce(dlmm_metrics.dlmm_spot_fees, 0) as dlmm_spot_fees
    , coalesce(amm_metrics.fees, 0) + coalesce(dlmm_metrics.dlmm_spot_fees, 0) as spot_fees
    , coalesce(amm_metrics.fees, 0) + coalesce(dlmm_metrics.dlmm_spot_fees, 0) as gross_protocol_revenue
    , (coalesce(dlmm_metrics.dlmm_spot_fees, 0) * .05) + coalesce(amm_metrics.fees, 0) * .20 as treasury_cash_flow
    , (coalesce(dlmm_metrics.dlmm_spot_fees, 0) * .95) + coalesce(amm_metrics.fees, 0) * .80 as service_cash_flow

from date_spine
left join dlmm_metrics using(date)
left join amm_metrics using(date)
left join spot_metrics using(date)
where date < to_date(sysdate())
