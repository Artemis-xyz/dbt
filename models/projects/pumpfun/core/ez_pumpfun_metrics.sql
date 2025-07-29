{{
    config(
        materialized='incremental',
        snowflake_warehouse='PUMPFUN',
        database='PUMPFUN',
        schema='core',
        alias='ez_metrics',
        incremental_strategy="merge",
        unique_key="date",
        on_schema_change="append_new_columns",
        merge_update_columns=var("backfill_columns", []),
        merge_exclude_columns=["created_on"] if not var("backfill_columns", []) else none,
        full_refresh=false,
        tags=["ez_metrics"]
    )
 }}

with date_spine as (
    select date_spine.date
    from {{ ref('dim_date_spine') }} date_spine
    where date_spine.date between '2023-10-01' and (to_date(sysdate()) - 1)
),

trades as (
    select *
    from {{ ref('fact_pumpfun_trades') }}
),
swap_metrics as (
    select 
        date,
        'pump.fun' as version, 
        COUNT(distinct tx_id) as number_of_swaps, 
        COUNT(distinct trader) as unique_traders, 
        SUM(amount) as trading_volume_sol, 
        SUM(amount_usd_artemis) as trading_volume_usd, 
        AVG(amount_usd_artemis) as average_traded_volume_usd 
    from trades
    group by 1, 2
),
daily_revenues as (
    select
        date,
        'pump.fun' as version,
        fees as launchpad_fees
    from {{ ref('fact_pumpfun_dailyrevenues') }}
),
pumpswap_metrics as (
    select
        date,
        'pumpswap' as version,
        spot_dau,
        spot_txns,
        spot_volume,
        spot_fees
    from {{ ref('fact_pumpswap_metrics') }}
)

-- Final combined query with one row per day
select
    date_spine.date,
    --Standardized Metrics
    coalesce(swap_metrics.unique_traders, 0) as launchpad_dau,
    coalesce(pumpswap_metrics.spot_dau, 0) as spot_dau,
    coalesce(swap_metrics.number_of_swaps, 0) as launchpad_txns,
    coalesce(pumpswap_metrics.spot_txns, 0) as spot_txns,
    coalesce(swap_metrics.trading_volume_usd, 0) as launchpad_volume,
    coalesce(pumpswap_metrics.spot_volume, 0) as spot_volume,
    coalesce(daily_revenues.launchpad_fees, 0) as launchpad_fees,
    coalesce(pumpswap_metrics.spot_fees, 0) as spot_fees,
    coalesce(daily_revenues.launchpad_fees, 0) + coalesce(pumpswap_metrics.spot_fees, 0) as ecosystem_revenue,
    -- timestamp columns
    TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on,
    TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on
from date_spine
left join swap_metrics using(date)
left join daily_revenues using(date)
left join pumpswap_metrics using(date)
where true
{{ ez_metrics_incremental('date_spine.date', backfill_date) }}
and date_spine.date < to_date(sysdate())
order by date desc