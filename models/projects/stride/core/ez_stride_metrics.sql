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

    market_data as ({{ get_coingecko_metrics("stride") }})

select
    fundamental_data.date
    , 'stride' as app
    , 'DeFi' as category
    
    --Market Data
    , market_data.price
    , market_data.market_cap
    , market_data.fdmc
    , market_data.token_volume

    --Chain Data
    , fundamental_data.dau as chain_dau
    , fundamental_data.wau as chain_wau
    , fundamental_data.mau as chain_mau
    , fundamental_data.returning_users as returning_users
    , fundamental_data.new_users as new_users
    , fundamental_data.low_sleep_users as low_sleep_users
    , fundamental_data.high_sleep_users as high_sleep_users
    , fundamental_data.sybil_users as sybil_users
    , fundamental_data.non_sybil_users as non_sybil_users
    , fundamental_data.txns as chain_txns
    , fundamental_data.tvl as lst_tvl
    , fundamental_data.tvl as tvl

    -- Fee Data
    , fundamental_data.avg_txn_fee
    , fundamental_data.fees_native as chain_fees_native
    , fundamental_data.fees_usd as chain_fees
    , fundamental_data.total_staking_yield_usd as yield_generated
    , (fundamental_data.total_staking_yield_usd * .1) as staking_reward_fees
    , chain_fees + staking_reward_fees as fees

    -- Fee allocations
    , (fundamental_data.total_staking_yield_usd * .08) as burn_fee_allocation
    , (fundamental_data.total_staking_yield_usd * .02) as service_fee_allocation

    -- Financial Statement Metrics
    , fundamental_data.total_supply_side_revenue_usd
    , fundamental_data.protocol_revenue_usd * 0.8 as revenue
    , 0 as operating_expenses_usd
    , fundamental_data.protocol_revenue_usd * 0.8  AS earnings

    -- Other Metrics
    , market_data.token_turnover_circulating
    , market_data.token_turnover_fdv

from fundamental_data
left join market_data on fundamental_data.date = market_data.date
where fundamental_data.date < to_date(sysdate())