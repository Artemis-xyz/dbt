{{
    config(
        materialized="table",
        snowflake_warehouse="STRIDE",
        database="stride",
        schema="core",
        alias="ez_metrics_by_chain",
    )
}}

with
    -- Alternative fundamental source from BigQuery, preferred when possible over Snowflake data
    fundamental_data as (select * EXCLUDE date, TO_TIMESTAMP_NTZ(date) AS date from {{ source('PROD_LANDING', 'ez_stride_metrics') }})

select
    fundamental_data.date,
    'stride' as app,
    'DeFi' as category,
    fundamental_data.chain,
    
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
    
    --Chain Usage Metrics
    , fundamental_data.dau as chain_dau
    , fundamental_data.wau as chain_wau
    , fundamental_data.mau as chain_mau
    , fundamental_data.txns as chain_txns
    , fundamental_data.returning_users as chain_returning_users
    , fundamental_data.new_users as chain_new_users
    , fundamental_data.low_sleep_users as chain_low_sleep_users
    , fundamental_data.avg_txn_fee as chain_avg_txn_fee

    
    --Cashflow Metrics
    , fundamental_data.fees_usd as chain_fees
    , fundamental_data.total_staking_yield_usd as yield_generated
    , chain_fees + yield_generated as ecosystem_revenue
    , (yield_generated * .1) as treasury_cash_flow
    , (yield_generated * .9) as service_cash_flow
    , chain_fees as validator_cash_flow    
    
from fundamental_data