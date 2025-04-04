{{
    config(
        materialized="table",
        snowflake_warehouse="CENTRIFUGE",
        database="centrifuge",
        schema="core",
        alias="ez_metrics",
    )
}}
with
    fundamental_data as (
        select
            date, 
            txns,
            daa, 
            fees_native, 
            fees_usd
        from {{ ref("fact_centrifuge_fundamental_metrics") }}
    )
    , market_data as (
        {{ get_coingecko_metrics("centrifuge") }}
    )
select
    date
    , txns
    , daa as dau
    , coalesce(fees_native, 0) as fees_native
    , coalesce(fees_usd, 0) as fees

    -- Standardized Metrics

    -- Token Metrics
    , coalesce(market_data.price, 0) as price
    , coalesce(market_data.market_cap, 0) as market_cap
    , coalesce(market_data.fdmc, 0) as fdmc
    , coalesce(market_data.token_volume, 0) as token_volume 

    -- Chain Metrics
    , daa as chain_dau
    , txns as chain_txns
    , coalesce(fees_native, 0) as l1_cash_flow_native
    , coalesce(fees_usd, 0) as l1_cash_flow
    , coalesce(fees_usd, 0)/coalesce(txns, 1) as chain_avg_txn_fee

    -- Cash Flow Metrics
    , coalesce(fees_usd, 0) as gross_protocol_revenue
    , coalesce(fees_native, 0) as gross_protocol_revenue_native

    -- Turnover Metrics
    , coalesce(market_data.token_turnover_circulating, 0) as token_turnover_circulating
    , coalesce(market_data.token_turnover_fdv, 0) as token_turnover_fdv
from fundamental_data
left join market_data using (date)
where fundamental_data.date < to_date(sysdate())
