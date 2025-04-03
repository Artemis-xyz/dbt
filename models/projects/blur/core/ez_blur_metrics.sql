{{
    config(
        materialized = 'table',
        snowflake_warehouse = 'BLUR',
        database = 'blur',
        schema = 'core',
        alias = 'ez_metrics'
    )
}}

with
    blur_fees as (
        select *
        from {{ ref("fact_blur_fees") }}
    )
    , blur_daus as (
        select *
        from {{ ref("fact_blur_daus") }}
    )
    , blur_daily_txns as (
        select *
        from {{ ref("fact_blur_daily_txns") }}
    )
    , market_data as (
        {{ get_coingecko_metrics("blur") }}
    )

select
    blur_daus.date
    , blur_daus.dau
    , blur_daily_txns.daily_txns as txns
    , blur_fees.fees

    -- Standardized Metrics

    -- Spot Dex Metrics
    , blur_daus.dau as spot_dau
    , blur_daily_txns.daily_txns as spot_txns
    , blur_fees.fees as spot_revenue

     -- Cash Flow Metrics
    , blur_fees.fees as gross_protocol_revenue
    , blur_fees.fees as foundation_cash_flow

    -- Token Metrics
    , coalesce(market_data.price, 0) as price
    , coalesce(market_data.market_cap, 0) as market_cap
    , coalesce(market_data.fdmc, 0) as fdmc
    , coalesce(market_data.token_turnover_circulating, 0) as token_turnover_circulating
    , coalesce(market_data.token_turnover_fdv, 0) as token_turnover_fdv
    , coalesce(market_data.token_volume, 0) as token_volume 

from blur_daus
left join blur_daily_txns using (date)
left join blur_fees using (date)
left join market_data using (date)
order by date desc
