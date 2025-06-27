{{
    config(
        materialized="table",
        snowflake_warehouse="HIVEMAPPER",
        database="hivemapper",
        schema="core",
        alias="ez_metrics_by_chain",
    )
}}

select
    date
    , 'solana' as chain
    , primary_supply_side_revenue
    , revenue
    , burns_native
    , dau

    -- Standardized Metrics

    -- Chain Metrics
    , chain_dau
    
    -- Cash Flow Metrics
    , fees
    , service_fee_allocation
    , burned_fee_allocation
    , burned_fee_allocation_native

    -- Supply Metrics
    , gross_emissions_native
    , gross_emissions
    , net_supply_change_native
from {{ ref('ez_hivemapper_metrics') }}