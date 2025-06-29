{{
    config(
        materialized="table",
        snowflake_warehouse="GEODNET",
        database="geodnet",
        schema="core",
        alias="ez_metrics_by_chain",
    )
}}

with
    revenue_data as (
        select date, fees, revenue, chain, protocol
        from {{ ref("fact_geodnet_fees_revenue") }}
    ),
    price_data as ({{ get_coingecko_metrics("geodnet") }})
select
    revenue_data.date
    , revenue_data.chain
    , revenue_data.protocol
    , coalesce(revenue, 0) as revenue

    -- Standardized Metrics

    -- Token Metrics
    , coalesce(price, 0) as price
    , coalesce(market_cap, 0) as market_cap
    , coalesce(fdmc, 0) as fdmc
    , coalesce(token_volume, 0) as token_volume

    -- Cash Flow Metrics
    , coalesce(fees, 0) as fees
    , coalesce(revenue, 0) * 0.8 as buyback_fee_allocation
    , coalesce(revenue, 0) * 0.2 as foundation_fee_allocation

    -- Turnover Metrics
    , coalesce(token_turnover_circulating, 0) as token_turnover_circulating
    , coalesce(token_turnover_fdv, 0) as token_turnover_fdv
from revenue_data
left join price_data on revenue_data.date = price_data.date
where revenue_data.date < to_date(sysdate())
