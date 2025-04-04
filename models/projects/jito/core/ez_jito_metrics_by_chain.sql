{{
    config(
        materialized='view',
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
    tip_fees,
    fees,
    revenue,
    supply_side_fees,
    txns,
    dau,
    tvl,
    amount_staked_usd,
    amount_staked_usd_net_change

    --Standardized Metrics
    , coalesce(withdraw_management_fees, 0) as lst_participating_token_cash_flow
    , coalesce(tip_fees, 0) as ecosystem_fees
    , coalesce(withdraw_management_fees, 0) + coalesce(tip_fees, 0) as gross_protocol_revenue
    , coalesce(tip_revenue, 0) + coalesce(withdraw_management_fees, 0) as ecosystem_revenue
    , coalesce(tip_supply_side_fees, 0) as lp_fees
    , coalesce(tip_txns, 0) as ecosystem_txns
    , coalesce(tip_dau, 0) as ecosystem_dau
    , coalesce(tvl, 0) as tvl
    , coalesce(tvl, 0) as tvl_native
    , coalesce(tvl_change, 0) as tvl_native_net_change
FROM {{ ref('ez_jito_metrics') }}