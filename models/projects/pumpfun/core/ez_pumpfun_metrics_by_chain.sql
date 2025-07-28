{{
    config(
        materialized='table',
        snowflake_warehouse='PUMPFUN',
        database='PUMPFUN',
        schema='core',
        alias='ez_metrics_by_chain',
    )
 }}

with date_spine as (
    select date_spine.date
    from {{ ref('dim_date_spine') }} date_spine
    where date_spine.date between '2023-10-01' and (to_date(sysdate()) - 1)
)

, pumpswap_metrics as (
    select
        date,
        'pumpswap' as version,
        spot_dau,
        spot_txns,
        spot_volume,
        spot_fees as spot_protocol_fees,
        spot_lp_fees
    from {{ ref('fact_pumpswap_metrics') }}
)
, pumpfun_metrics as (
    select
        date,
        launchpad_dau,
        launchpad_txns,
        launchpad_volume,
        launchpad_fees
    from {{ ref('fact_pumpfun_metrics') }}
)

select
    date_spine.date,
    'solana' as chain --pump is exclusively running to Solana

    --Standardized Metrics

    --Usage Metrics
    , pumpfun_metrics.launchpad_dau
    , pumpfun_metrics.launchpad_txns
    , pumpfun_metrics.launchpad_volume
    , pumpswap_metrics.spot_dau
    , pumpswap_metrics.spot_txns
    , pumpswap_metrics.spot_volume
    
    --Cashflow Metrics
    , pumpswap_metrics.spot_protocol_fees
    , pumpswap_metrics.spot_lp_fees
    , pumpswap_metrics.spot_protocol_fees + pumpswap_metrics.spot_lp_fees as spot_fees
    , pumpfun_metrics.launchpad_fees
    , pumpfun_metrics.launchpad_fees + coalesce(pumpswap_metrics.spot_protocol_fees, 0) + coalesce(pumpswap_metrics.spot_lp_fees, 0) as fees 

    -- Financial Statement Metrics
    , 0 as revenue

from date_spine
left join pumpfun_metrics using(date)
left join pumpswap_metrics using(date)
where date_spine.date < to_date(sysdate())
order by date desc