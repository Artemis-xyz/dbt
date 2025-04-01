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
    ds.date,
    'base' as chain,

    -- Old metrics needed for compatibility
    sm.unique_traders,
    sm.total_swaps,
    sm.daily_volume_usd as trading_volume,
    sm.daily_fees_usd as trading_fees,
    tm.tvl_usd as tvl

    -- Standardized Metrics
    sm.unique_traders as spot_dau,
    sm.total_swaps as spot_txns,
    sm.daily_volume_usd as spot_volume,
    sm.daily_fees_usd as spot_fees,
    sm.daily_fees_usd as gross_protocol_revenue,
    sm.daily_fees_usd as fee_sharing_token_cash_flow,
    tm.tvl_usd as tvl

FROM date_spine ds
LEFT JOIN swap_metrics sm using (date)
LEFT JOIN tvl_metrics tm using (date)