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
        circulating_supply
    FROM {{ ref('fact_aerodrome_supply_data') }}
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

    -- Money Metrics
    , coalesce(sm.daily_fees_usd, 0) as spot_fees
    , coalesce(sm.daily_fees_usd, 0) as gross_protocol_revenue
    , coalesce(sm.daily_fees_usd, 0) as fee_sharing_token_cash_flow

    -- Supply Metrics
    , coalesce(sp.pre_mine_unlocks, 0) + coalesce(sp.emissions_native, 0) as mints_native
    , (coalesce(sp.pre_mine_unlocks, 0) + coalesce(sp.emissions_native, 0)) * coalesce(mm.price, 0) as mints
    , coalesce(sp.emissions_native, 0) as emissions_native
    , coalesce(sp.pre_mine_unlocks, 0) as premine_unlocks_native
    , coalesce(sp.emissions_native, 0) as net_supply_change_native
    , coalesce(sp.circulating_supply, 0) as circulating_supply_native
    , coalesce(sp.locked_supply, 0) as locked_supply
    , coalesce(sp.total_supply, 0) as total_supply

    -- Other Metrics
    , coalesce(mm.token_turnover_circulating, 0) as token_turnover_circulating
    , coalesce(mm.token_turnover_fdv, 0) as token_turnover_fdv
FROM date_spine ds
LEFT JOIN swap_metrics sm using (date)
LEFT JOIN tvl_metrics tm using (date)
LEFT JOIN market_metrics mm using (date)
LEFT JOIN supply_metrics sp using (date)
WHERE ds.date < to_date(sysdate())