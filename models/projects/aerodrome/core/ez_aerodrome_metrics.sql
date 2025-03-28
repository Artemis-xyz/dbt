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
                    )
                )
        and to_date(sysdate())
)
SELECT
    ds.date,
    sm.unique_traders,
    sm.total_swaps as number_of_swaps,
    sm.daily_volume_usd as trading_volume,
    sm.daily_fees_usd as trading_fees,
    tm.tvl_usd as tvl

    -- Standardized Metrics
    , sm.unique_traders as spot_dau
    , sm.total_swaps as spot_txns
    , sm.daily_volume_usd as spot_volume
    , sm.daily_fees_usd as spot_revenue
    , sm.daily_fees_usd as ecosystem_revenue
    , sm.daily_fees_usd as fee_sharing_token_revenue

    -- Market Metrics
    , mm.price as price
    , mm.token_volume as token_volume
    , mm.market_cap as market_cap
    , mm.fdmc as fdmc
    , mm.token_turnover_circulating as token_turnover_circulating
    , mm.token_turnover_fdv as token_turnover_fdv
FROM date_spine ds
LEFT JOIN swap_metrics sm using (date)
LEFT JOIN tvl_metrics tm using (date)
LEFT JOIN market_metrics mm using (date)