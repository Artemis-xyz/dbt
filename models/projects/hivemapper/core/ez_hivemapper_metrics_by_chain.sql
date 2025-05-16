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
    , fees
    , primary_supply_side_revenue
    , revenue
    , burns_native
    , dau

    -- Standardized Metrics

    -- Chain Metrics
    , chain_dau
    
    -- Cash Flow Metrics
    , ecosystem_revenue
    , service_cash_flow
    , burned_cash_flow
    , burned_cash_flow_native

    -- Supply Metrics
    , gross_emissions_native
    , gross_emissions
    , net_supply_change_native
from {{ ref('ez_hivemapper_metrics') }}