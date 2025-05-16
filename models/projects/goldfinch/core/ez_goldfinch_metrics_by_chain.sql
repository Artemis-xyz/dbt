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
    , treasury_value
    , treasury_value_native
    , net_treasury_value
    , tvl_growth
    , token_holder_count

    -- Standardized Metrics

    -- Lending Metrics
    , lending_deposits
    , lending_loan_capacity
    , lending_interest_fees

    -- Crypto Metrics
    , tvl
    , tvl_net_change

    -- Cash Flow
    , ecosystem_revenue
    , service_cash_flow
    , token_cash_flow

    -- Protocol Metrics
    , treasury
    , treasury_native
    , net_treasury
    , net_treasury_native
    , own_token_treasury
    , own_token_treasury_native
from {{ ref('ez_goldfinch_metrics') }}
