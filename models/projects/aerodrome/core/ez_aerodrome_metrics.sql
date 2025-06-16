{{
    config(
        materialized='table',
        snowflake_warehouse='AERODROME',
        database='aerodrome',
        schema='core',
        alias='ez_metrics'
    )
}}

with swap_metrics as (
    SELECT
        block_timestamp::date as date,
        COUNT(DISTINCT sender) as unique_traders,
        COUNT(*) as total_swaps,
        SUM(amount_in_usd) as daily_volume_usd,
        SUM(fee_usd) as daily_fees_usd
    FROM {{ ref('fact_aerodrome_swaps') }}
    GROUP BY 1
)
, tvl_metrics as (
    SELECT
        date,
        SUM(token_balance_usd) as tvl_usd
    FROM {{ ref('fact_aerodrome_tvl') }}
    GROUP BY date
)
, market_metrics as (
    {{get_coingecko_metrics('aerodrome-finance')}}
)
, supply_metrics as (
    SELECT
        date,
        pre_mine_unlocks,
        emissions_native,
        locked_supply,
        total_supply,
        circulating_supply_native, 
        buybacks_native, 
        buybacks
    FROM {{ ref('fact_aerodrome_supply_data') }}
)
, pools_metrics as (
    SELECT
        date,
        cumulative_count
    FROM {{ ref('fact_aerodrome_pools') }}
)
, token_incentives as (
        select
            day as date,
            usd_value as token_incentives
        from {{ref('fact_aerodrome_token_incentives')}}
)
, date_spine as (
    SELECT
        ds.date
    FROM {{ ref('dim_date_spine') }} ds
    WHERE ds.date
        between (
                    select min(min_date) from (
                        select min(date) as min_date from swap_metrics
                        UNION ALL
                        select min(date) as min_date from tvl_metrics
                        UNION ALL
                        select min(date) as min_date from market_metrics
                        UNION ALL
                        select min(date) as min_date from supply_metrics
                        UNION ALL
                        select min(date) as min_date from pools_metrics
                    )
                )
        and to_date(sysdate())
)
SELECT
    ds.date

    -- Old metrics needed for compatibility
    , coalesce(sm.unique_traders, 0) as unique_traders
    , coalesce(sm.total_swaps, 0) as number_of_swaps
    , coalesce(sm.daily_volume_usd, 0) as trading_volume
    , coalesce(sm.daily_fees_usd, 0) as trading_fees

    -- Standardized Metrics
    -- Market Metrics
    , coalesce(mm.price, 0) as price
    , coalesce(mm.market_cap, 0) as market_cap
    , coalesce(mm.fdmc, 0) as fdmc
    , coalesce(mm.token_volume, 0) as token_volume

    -- Usage/Sector Metrics
    , coalesce(sm.unique_traders, 0) as spot_dau
    , coalesce(sm.total_swaps, 0) as spot_txns
    , coalesce(sm.daily_volume_usd, 0) as spot_volume
    , coalesce(tm.tvl_usd, 0) as tvl

    -- Cash Flow Metrics
    , coalesce(sm.daily_fees_usd, 0) as spot_fees
    , coalesce(sm.daily_fees_usd, 0) as fees
    , coalesce(sm.daily_fees_usd, 0) as staking_fee_allocation
    , coalesce(sp.buybacks_native, 0) as buybacks_native
    , coalesce(sp.buybacks, 0) as buybacks
    , (coalesce(sm.daily_fees_usd, 0) + coalesce(sp.buybacks, 0)) as revenue
    , (coalesce(sp.buybacks, 0)) - ti.token_incentives as earnings
    -- NOTE: We do not track bribes as a part of revenue here. 

    -- Supply Metrics
    , coalesce(sp.emissions_native, 0) as gross_emissions_native
    , coalesce(sp.emissions_native, 0) * coalesce(mm.price, 0) as gross_emissions
    , coalesce(sp.pre_mine_unlocks, 0) as premine_unlocks_native
    , coalesce(sp.circulating_supply_native, 0) - lag(coalesce(sp.circulating_supply_native, 0)) over (order by date) as net_supply_change_native
    , coalesce(sp.circulating_supply_native, 0) as circulating_supply_native
    , coalesce(sp.locked_supply, 0) as locked_supply
    , coalesce(sp.total_supply, 0) as total_supply

    -- Other Metrics
    , coalesce(mm.token_turnover_circulating, 0) as token_turnover_circulating
    , coalesce(mm.token_turnover_fdv, 0) as token_turnover_fdv
    , coalesce(pm.cumulative_count, 0) as total_pools
    , coalesce(ti.token_incentives, 0) as token_incentives

FROM date_spine ds
LEFT JOIN swap_metrics sm using (date)
LEFT JOIN tvl_metrics tm using (date)
LEFT JOIN market_metrics mm using (date)
LEFT JOIN supply_metrics sp using (date)
LEFT JOIN pools_metrics pm using (date)
LEFT JOIN token_incentives ti using (date)
WHERE ds.date < to_date(sysdate())