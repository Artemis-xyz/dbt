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
    'jito' as artemis_id,

    --Standardized Metrics
    --Market Metrics
    , price
    , token_volume
    , market_cap
    , fdmc
    
    --Usage Metrics
    , block_infra_txns
    , block_infra_dau
    , lst_tvl as lst_tvl
    , tvl as tvl

    --Fee Metrics
    , lst_fees
    , block_infra_fees
    , fees
    , equity_fee_allocation
    , treasury_fee_allocation
    , strategy_fee_allocation
    , validator_fee_allocation

    -- Financial Metrics
    , revenue
FROM {{ ref('ez_jito_metrics') }}