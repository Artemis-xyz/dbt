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
    withdraw_management_fees,
    tip_fees,
    fees,
    revenue,
    supply_side_fees,
    txns,
    dau,
    tvl
FROM {{ ref('ez_jito_metrics') }}