{{
    config(
        materialized="view"
        , snowflake_warehouse="GOLDFINCH"
        , database="goldfinch"
        , schema="core"
        , alias="ez_metrics_by_chain"
    )
}}

select
    date
    , 'ethereum' as chain
    , interest_fees
    , withdrawal_fees
    , fees
    , primary_supply_side_revenue
    , secondary_supply_side_revenue
    , total_supply_side_revenue
    , interest_revenue
    , withdrawal_revenue
    , revenue
    , token_incentives
    , operating_expenses
    , total_expenses
    , protocol_earnings
    , net_deposits
    , outstanding_supply
    , net_treasury_value
    , tvl_growth
    , token_holder_count

    -- Lending Metrics
    , lending_deposits
    , lending_loan_capacity

    -- Crypto Metrics
    , tvl
    , tvl_net_change

    -- Cash Flow
    , gross_protocol_revenue
    , service_cash_flow
    , token_cash_flow
    , foundation_cash_flow
    
    -- Protocol Metrics
    , treasury
    , treasury_native
    , treasury_native_net_change

from {{ ref('ez_goldfinch_metrics') }}
