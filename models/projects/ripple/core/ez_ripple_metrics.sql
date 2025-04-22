{{
    config( 
        materialized="table",
        snowflake_warehouse="RIPPLE",
        database="ripple",
        schema="core",
        alias="ez_metrics",
    )
}}

with
    fundamental_data as (
        select
            date,
            chain_fees,
            chain_dau,
            chain_txns
        from {{ ref("fact_ripple_fundamental_metrics") }}
    )
    , price_data as ({{ get_coingecko_metrics("ripple") }})
    
select
    fundamental_data.date -- goes back to Jan 2013

    -- Old metrics for compatibility
    , chain_dau as dau
    , chain_txns as txns
    , chain_fees as fees
    , chain_fees as revenue

    -- Market Metrics
    , price_data.price
    , price_data.market_cap
    , price_data.fdmc
    , price_data.token_volume

    -- Usage Metrics
    , chain_dau
    , chain_txns
    
    -- Cash Flow Metrics
    , chain_fees
    , chain_fees as gross_protocol_revenue
    , chain_fees as burned_cash_flow

    -- Other Metrics
    , price_data.token_turnover_circulating
    , price_data.token_turnover_fdv
FROM fundamental_data
left join price_data on fundamental_data.date = price_data.date
where fundamental_data.date < to_date(sysdate())