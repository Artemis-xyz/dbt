{{
    config(
        materialized = 'view',
        snowflake_warehouse = 'MAPLE',
        database = 'MAPLE',
        schema = 'core',
        alias = 'ez_metrics_by_chain'
    )
}}

SELECT
    date
    , 'ethereum' as chain
    , interest_fees
    , primary_supply_side_revenue
    , total_supply_side_revenue
    , revenue
    , token_incentives
    , total_expenses
    , protocol_earnings
    , treasury_value
    , treasury_value_native
    , net_treasury_value
    , net_deposits
    , outstanding_supply
    , token_holder_count

    -- Standardized Metrics

    -- Token Metrics
    , price
    , market_cap
    , fdmc
    , token_volume

    -- Lending Metrics
    , lending_deposits
    , lending_fees
    , lending_loans
    , lending_loan_capacity

    -- Crypto Metrics
    , tvl
    , tvl_net_change

    -- Cash Flow Metrics
    , gross_protocol_revenue
    , fee_sharing_token_cash_flow 
    , service_cash_flow
    , token_cash_flow
    , treasury_cash_flow

    -- Protocol Metrics
    , treasury
    , treasury_native
    , net_treasury
    , net_treasury_native
    , own_token_treasury
    , own_token_treasury_native

    -- Turnover Metrics
    , token_turnover_circulating
    , token_turnover_fdv
FROM {{ ref('ez_maple_metrics') }}