{{
    config(
        materialized='table',
        snowflake_warehouse='jito',
        database='jito',
        schema='core',
        alias='ez_metrics_by_chain'
    )
}}

SELECT
    date,
    'solana' as chain,

    --Old metrics needed for compatibility
    withdraw_management_fees,
    fees,
    revenue,
    supply_side_fees,
    txns,
    dau,
    amount_staked_usd,
    amount_staked_usd_net_change

    --Standardized Metrics
    , coalesce(withdraw_management_fees, 0) as lst_fees
    , coalesce(tip_fees, 0) as tip_fees
    , coalesce(withdraw_management_fees, 0) + coalesce(tip_fees, 0) as ecosystem_revenue
    , coalesce(equity_fee_allocation, 0) as equity_fee_allocation
    , coalesce(treasury_fee_allocation, 0) as treasury_fee_allocation
    , coalesce(strategy_fee_allocation, 0) as strategy_fee_allocation
    , coalesce(validator_fee_allocation, 0) as validator_fee_allocation
    , coalesce(block_infra_txns, 0) as block_infra_txns
    , coalesce(block_infra_dau, 0) as block_infra_dau
    , coalesce(tvl, 0) as lst_tvl
    , coalesce(tvl, 0) as tvl
    , coalesce(tvl - lag(tvl) over (partition by chain order by date), 0) as lst_tvl_net_change

FROM {{ ref('ez_jito_metrics') }}