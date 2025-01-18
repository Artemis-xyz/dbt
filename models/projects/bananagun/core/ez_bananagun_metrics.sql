{{
    config(
        materialized='table',
        snowflake_warehouse='BANANAGUN',
        database='BANANAGUN',
        schema='core',
        alias='ez_metrics'
    )
}}

with metrics as (
    SELECT 
        trade_date,
        SUM(trading_volume) as trading_volume,
        SUM(dau) as dau,
        SUM(daily_txns) as daily_txns,
        SUM(fees_usd) as fees_usd
    FROM {{ ref('fact_bananagun_all_metrics') }}
    GROUP BY trade_date
)
, burns as (
    select date, burn_amount_usd as revenue from {{ ref('fact_bananagun_coin_metrics') }}
)

select
    metrics.trade_date,
    metrics.trading_volume,
    metrics.dau,
    metrics.daily_txns,
    metrics.fees_usd as fees,
    metrics.fees_usd * 0.6 as supply_side_fees,
    metrics.fees_usd * 0.4 + burns.revenue as revenue
from metrics
left join burns on metrics.trade_date = burns.date
order by metrics.trade_date desc