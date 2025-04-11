{{
    config(
        materialized="table",
        snowflake_warehouse="JUPITER",
        database="jupiter",
        schema="core",
        alias="ez_metrics",
    )
}}

with
    all_trade_metrics as (
       select
        date,

        -- Fees
        SUM(CASE WHEN trade_type = 'perps' THEN fees ELSE 0 END) as perp_fees, -- Perps specific metric
        SUM(CASE WHEN trade_type = 'aggregator' THEN fees ELSE 0 END) as aggregator_fees, -- Aggregator specific metric
        SUM(CASE WHEN trade_type = 'dca' THEN fees ELSE 0 END) as dca_fees,
        SUM(CASE WHEN trade_type = 'limit_order' THEN fees ELSE 0 END) as limit_order_fees,
        sum(fees) as fees,

        -- Revenue
        sum(CASE WHEN trade_type = 'perps' THEN revenue ELSE 0 END) as perp_revenue, -- Perps specific metric
        sum(CASE WHEN trade_type = 'aggregator' THEN revenue ELSE 0 END) as aggregator_revenue, -- Aggregator specific metric
        sum(CASE WHEN trade_type = 'dca' THEN revenue ELSE 0 END) as dca_revenue,
        sum(CASE WHEN trade_type = 'limit_order' THEN revenue ELSE 0 END) as limit_order_revenue,
        sum(revenue) as revenue,
        sum(CASE WHEN date >= '2025-02-17' THEN revenue * 0.5 ELSE 0 END) as buyback,

        -- Supply Side Revenue
        sum(CASE WHEN trade_type = 'perps' THEN supply_side_revenue ELSE 0 END) as perp_supply_side_revenue,
        SUM(CASE WHEN trade_type = 'perps' THEN supply_side_revenue ELSE 0 END) as primary_supply_side_revenue,
        sum(supply_side_revenue) as total_supply_side_revenue,

        -- Volume
        sum(CASE WHEN trade_type = 'perps' THEN volume ELSE 0 END) as trading_volume, -- Perps specific metric
        sum(CASE WHEN trade_type = 'aggregator' THEN volume ELSE 0 END) as aggregator_volume, -- Aggregator specific metric
        sum(CASE WHEN trade_type = 'dca' THEN volume ELSE 0 END) as dca_volume,
        sum(CASE WHEN trade_type = 'limit_order' THEN volume ELSE 0 END) as limit_order_volume,
        sum(volume) as volume,

        -- Txns
        sum(CASE WHEN trade_type = 'aggregator' THEN txns ELSE 0 END) as aggregator_txns,
        sum(CASE WHEN trade_type = 'perps' THEN txns ELSE 0 END) as perp_txns,
        sum(CASE WHEN trade_type = 'dca' THEN txns ELSE 0 END) as dca_txns,
        sum(CASE WHEN trade_type = 'limit_order' THEN txns ELSE 0 END) as limit_order_txns,
        sum(txns) as txns,

        -- DAU
        sum(CASE WHEN trade_type = 'perps' THEN dau ELSE 0 END) as unique_traders, -- Perps specific metric
        sum(CASE WHEN trade_type = 'aggregator' THEN dau ELSE 0 END) as aggregator_unique_traders, -- Aggregator specific metric
        sum(dau) as dau
       from {{ ref("fact_jupiter_all_trade_metrics") }}
       where date < to_date(sysdate())
       group by 1
    ),
    price_data as ({{ get_coingecko_metrics("jupiter-exchange-solana") }})

select
    all_trade_metrics.date as date
    , 'solana' as chain
    , 'jupiter' as protocol

    -- Old metrics needed for compatibility
    -- Fees
    , all_trade_metrics.fees

    -- Revenue
    , all_trade_metrics.perp_revenue
    , all_trade_metrics.aggregator_revenue
    , all_trade_metrics.revenue

    -- Volume
    , all_trade_metrics.trading_volume

    -- DAU
    , all_trade_metrics.unique_traders -- perps specific metric
    , all_trade_metrics.aggregator_unique_traders -- aggregator specific metric
    , all_trade_metrics.txns

    -- Standardized Metrics

    -- Volume
    , all_trade_metrics.aggregator_volume
    , all_trade_metrics.trading_volume as perp_volume
    , all_trade_metrics.dca_volume
    , all_trade_metrics.limit_order_volume

    -- Txns
    , all_trade_metrics.aggregator_txns
    , all_trade_metrics.perp_txns
    , all_trade_metrics.dca_txns
    , all_trade_metrics.limit_order_txns

    -- DAU
    , all_trade_metrics.unique_traders as perp_dau -- perps specific metric
    , all_trade_metrics.aggregator_unique_traders as aggregator_dau -- aggregator specific metric
    , aggregator_dau + perp_dau as dau -- necessary for OL index pipeline

    -- Fees
    , all_trade_metrics.perp_fees
    , all_trade_metrics.aggregator_fees
    , all_trade_metrics.dca_fees
    , all_trade_metrics.limit_order_fees

    -- Revenue
    , all_trade_metrics.fees as gross_protocol_revenue
    , all_trade_metrics.aggregator_fees - all_trade_metrics.aggregator_revenue as integrator_cash_flow
    , perp_supply_side_revenue as service_cash_flow
    , all_trade_metrics.revenue as treasury_cash_flow

    , all_trade_metrics.perp_revenue as perp_treasury_cash_flow
    , all_trade_metrics.aggregator_revenue as aggregator_treasury_cash_flow
    , all_trade_metrics.dca_revenue as dca_treasury_cash_flow
    , all_trade_metrics.limit_order_revenue as limit_order_treasury_cash_flow
    , all_trade_metrics.buyback as buyback

    -- Market Data
    , price
    , market_cap
    , fdmc
    , token_turnover_circulating
    , token_turnover_fdv
    , token_volume

from all_trade_metrics
left join price_data using (date)
where all_trade_metrics.date < to_date(sysdate())
