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
    fundamental_data.date
    , 'stride' as app
    , 'DeFi' as category
    , fundamental_data.chain
    
    --Chain Data
    , fundamental_data.dau as chain_dau
    , fundamental_data.wau as chain_wau
    , fundamental_data.mau as chain_mau
    , fundamental_data.txns as chain_txns
    , fundamental_data.returning_users as chain_returning_users
    , fundamental_data.new_users as chain_new_users
    , fundamental_data.low_sleep_users as chain_low_sleep_users
    , fundamental_data.avg_txn_fee as chain_avg_txn_fee
    , fundamental_data.sybil_users
    , fundamental_data.non_sybil_users
    , fundamental_data.tvl as lst_tvl
    , fundamental_data.tvl as tvl
    
    --Fee Data
    , fundamental_data.avg_txn_fee
    , fundamental_data.fees_native AS chain_fees_native
    , fundamental_data.fees_usd AS chain_fees
    , fundamental_data.total_staking_yield_usd as yield_generated
    , (fundamental_data.total_staking_yield_usd * .1) as staking_reward_fees
    , chain_fees + staking_reward_fees as fees

    --Fee Allocations
    , (fundamental_data.total_staking_yield_usd * .08) as burn_fee_allocation
    , (fundamental_data.total_staking_yield_usd * .02) as service_fee_allocation

    --Financial Statement Metrics
    , fundamental_data.total_supply_side_revenue_usd
    , fundamental_data.protocol_revenue_usd * 0.8 as revenue
    , 0 as operating_expenses_usd
    , fundamental_data.protocol_revenue_usd * 0.8  AS earnings

    
from fundamental_data