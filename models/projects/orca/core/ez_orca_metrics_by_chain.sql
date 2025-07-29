{{
    config(
        materialized="table",
        database = 'orca',
        schema = 'core',
        snowflake_warehouse = 'ORCA',
        alias = 'ez_metrics_by_chain'
    )
}}

select
    date
    , 'orca' as artemis_id
    , 'solana' as chain

    -- Standardized Metrics
    -- Usage/Sector Metrics
    , spot_dau
    , spot_txns
    , spot_volume
    , tvl

    -- Money Metrics
    , treasury_fee_allocation
    , fees
    , lp_fee_allocation
    , other_fee_allocation
from {{ ref("ez_orca_metrics") }}