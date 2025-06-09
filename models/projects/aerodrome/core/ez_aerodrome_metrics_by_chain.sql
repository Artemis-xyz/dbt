{{
    config(
        materialized='table',
        snowflake_warehouse='AERODROME',
        database='aerodrome',
        schema='core',
        alias='ez_metrics_by_chain'
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
                    )
                )
        and to_date(sysdate())
)
SELECT
    ds.date
    , 'base' as chain

    -- Old metrics needed for compatibility
    , sm.unique_traders
    , sm.total_swaps
    , sm.daily_volume_usd as trading_volume
    , sm.daily_fees_usd as trading_fees

    -- Standardized Metrics
    -- Usage/Sector Metrics
    , sm.unique_traders as spot_dau
    , sm.total_swaps as spot_txns
    , sm.daily_volume_usd as spot_volume
    , tm.tvl_usd as tvl

    -- Money Metrics
    , sm.daily_fees_usd as spot_fees
    , sm.daily_fees_usd as ecosystem_revenue
    , sm.daily_fees_usd as staking_cash_flow
    , coalesce(ti.token_incentives, 0) as token_incentives
FROM date_spine ds
LEFT JOIN swap_metrics sm using (date)
LEFT JOIN tvl_metrics tm using (date)
LEFT JOIN token_incentives ti using (date)
