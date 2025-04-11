{{
    config(
        materialized="table",
        snowflake_warehouse="STRIDE",
        database="stride",
        schema="core",
        alias="ez_metrics",
    )
}}

with
    -- Alternative fundamental source from BigQuery, preferred when possible over Snowflake data
    fundamental_data as (select * EXCLUDE date, TO_TIMESTAMP_NTZ(date) AS date from {{ source('PROD_LANDING', 'ez_stride_metrics') }}),

    market_data as ({{ get_coin_metrics("stride") }}),

select
    fundamental_data.date,
    'stride' as app,
    'DeFi' as category,
    
    --Old metrics needed for compatibility
    fundamental_data.txns,
    fundamental_data.dau,
    fundamental_data.wau,
    fundamental_data.mau,
    fundamental_data.tvl,
    fundamental_data.tvl_net_change,
    fundamental_data.fees_native AS gas_fees_native,
    fundamental_data.fees_usd AS gas_fees_usd,
    fundamental_data.avg_txn_fee,
    fundamental_data.returning_users,
    fundamental_data.new_users,
    fundamental_data.low_sleep_users,
    fundamental_data.high_sleep_users,
    fundamental_data.sybil_users,
    fundamental_data.non_sybil_users,
    fundamental_data.total_staking_yield_usd,
    fundamental_data.total_supply_side_revenue_usd,
    fundamental_data.protocol_revenue_usd AS fees,
    fundamental_data.protocol_revenue_usd AS revenue,
    fundamental_data.operating_expenses_usd,
    fundamental_data.protocol_earnings_usd

    --Standardized Metrics

    --Market Data Metrics
    , market_data.price
    , market_data.market_cap
    , market_data.fdmc
    , fundamental_data.tvl as tvl
    , fundamental_data.tvl_net_change as tvl_net_change

    --Chain Usage Metrics
    , fundamental_data.dau as chain_dau
    , fundamental_data.wau as chain_wau
    , fundamental_data.mau as chain_mau
    , fundamental_data.txns as chain_txns
    , fundamental_data.returning_users as returning_users
    , fundamental_data.new_users as new_users
    , fundamental_data.low_sleep_users as low_sleep_users
    , fundamental_data.high_sleep_users as high_sleep_users
    , fundamental_data.sybil_users as sybil_users
    , fundamental_data.non_sybil_users as non_sybil_users

    --Cashflow Metrics
    , fundamental_data.fees_native as chain_fees_native
    , fundamental_data.fees_usd as chain_fees
    , fundamental_data.avg_txn_fee as chain_avg_txn_fee
    , fundamental_data.total_staking_yield_usd as gross_staking_revenue
    , chain_fees + gross_staking_revenue as gross_protocol_revenue
    , fundamental_data.protocol_revenue_usd as protocol_staking_revenue
    , chain_fees as protocol_fees_revenue
    , protocol_staking_revenue + chain_fees as total_protocol_revenue
    , fundamental_data.total_supply_side_revenue_usd as supply_side_staking_revenue
    , fundamental_data.operating_expenses_usd as operating_expenses
    , fundamental_data.protocol_earnings_usd as protocol_earnings
from fundamental_data
left join market_data on fundamental_data.date = market_data.date
where fundamental_data.date < to_date(sysdate())