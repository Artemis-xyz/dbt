{{
    config(
        materialized="table",
        snowflake_warehouse="AAVE",
        database="aave",
        schema="core",
        alias="ez_metrics_by_token",
    )
}}
-- Liquidation Revenue can not be calculated by token. This is becuase the revenue is based on
-- the difference between the collateral revieved and the debt repaid
with
    deposits_borrows_lender_revenue as (
        select * from {{ref("fact_aave_v3_arbitrum_deposits_borrows_lender_revenue")}}
        union all
        select * from {{ref("fact_aave_v2_avalanche_deposits_borrows_lender_revenue")}}
        union all
        select * from {{ref("fact_aave_v3_avalanche_deposits_borrows_lender_revenue")}}
        union all
        select * from {{ref("fact_aave_v3_base_deposits_borrows_lender_revenue")}}
        union all 
        select * from {{ref("fact_aave_v3_bsc_deposits_borrows_lender_revenue")}}
        union all
        select * from {{ref("fact_aave_v2_ethereum_deposits_borrows_lender_revenue")}}
        union all
        select * from {{ref("fact_aave_v3_ethereum_deposits_borrows_lender_revenue")}}
        union all
        select * from {{ref("fact_aave_v3_gnosis_deposits_borrows_lender_revenue")}}
        union all
        select * from {{ref("fact_aave_v3_optimism_deposits_borrows_lender_revenue")}}
        union all
        select * from {{ref("fact_aave_v2_polygon_deposits_borrows_lender_revenue")}}
        union all
        select * from {{ref("fact_aave_v3_polygon_deposits_borrows_lender_revenue")}}
    )
    , aave_outstanding_supply_net_deposits_deposit_revenue as (
        select
            date
            , chain
            , token_address
            , sum(borrows) as outstanding_supply_nominal
            , sum(borrows_usd) as outstanding_supply
            , sum(supply) as net_deposits_nominal
            , sum(supply_usd) as net_deposits
            , coalesce(net_deposits_nominal, 0) - coalesce(outstanding_supply_nominal, 0) as tvl_nominal
            , coalesce(net_deposits, 0) - coalesce(outstanding_supply, 0) as tvl
            , sum(deposit_revenue) as supply_side_deposit_revenue
            , sum(deposit_revenue_nominal) as supply_side_deposit_revenue_nominal
            , sum(interest_rate_fees) as interest_rate_fees
            , sum(interest_rate_fees_nominal) as interest_rate_fees_nominal
            , sum(reserve_factor_revenue) as reserve_factor_revenue
            , sum(reserve_factor_revenue_nominal) as reserve_factor_revenue_nominal
        from deposits_borrows_lender_revenue
        group by 1, 2, 3
    )
    , flashloan_fees as (
        select * from {{ref("fact_aave_v3_arbitrum_flashloan_fees")}}
        union all
        select * from {{ref("fact_aave_v2_avalanche_flashloan_fees")}}
        union all
        select * from {{ref("fact_aave_v3_avalanche_flashloan_fees")}}
        union all
        select * from {{ref("fact_aave_v3_base_flashloan_fees")}}
        union all
        select * from {{ref("fact_aave_v2_ethereum_flashloan_fees")}}
        union all
        select * from {{ref("fact_aave_v3_ethereum_flashloan_fees")}}
        union all
        select * from {{ref("fact_aave_v3_gnosis_flashloan_fees")}}
        union all
        select * from {{ref("fact_aave_v3_optimism_flashloan_fees")}}
        union all
        select * from {{ref("fact_aave_v2_polygon_flashloan_fees")}}
        union all
        select * from {{ref("fact_aave_v3_polygon_flashloan_fees")}}
    )
    , aave_flashloan_fees as (
        select 
            date
            , chain
            , token_address
            , sum(amount_usd) as flashloan_fees
            , sum(amount_nominal) as flashloan_fees_nominal
        from flashloan_fees
        group by 1, 2, 3
    )
    , liquidation_revenue as (
        select * from {{ref("fact_aave_v3_arbitrum_liquidation_revenue")}}
        union all
        select * from {{ref("fact_aave_v2_avalanche_liquidation_revenue")}}
        union all
        select * from {{ref("fact_aave_v3_avalanche_liquidation_revenue")}}
        union all
        select * from {{ref("fact_aave_v3_base_liquidation_revenue")}}
        union all
        select * from {{ref("fact_aave_v3_bsc_liquidation_revenue")}}
        union all
        select * from {{ref("fact_aave_v2_ethereum_liquidation_revenue")}}
        union all
        select * from {{ref("fact_aave_v3_ethereum_liquidation_revenue")}}
        union all
        select * from {{ref("fact_aave_v3_gnosis_liquidation_revenue")}}
        union all
        select * from {{ref("fact_aave_v3_optimism_liquidation_revenue")}}
        union all
        select * from {{ref("fact_aave_v2_polygon_liquidation_revenue")}}
        union all
        select * from {{ref("fact_aave_v3_polygon_liquidation_revenue")}}
    )
    , aave_liquidation_supply_side_revenue as (
        select 
            date
            , chain
            , 'USD' as token_address
            , sum(liquidation_revenue) as liquidation_revenue
            , null as liquidation_revenue_nominal
        from liquidation_revenue
        group by 1, 2, 3
    )
    , ecosystem_incentives as (
        select * from {{ref("fact_aave_v3_arbitrum_ecosystem_incentives")}}
        union all
        select * from {{ref("fact_aave_v2_avalanche_ecosystem_incentives")}}
        union all
        select * from {{ref("fact_aave_v3_avalanche_ecosystem_incentives")}}
        union all
        select * from {{ref("fact_aave_v3_base_ecosystem_incentives")}}
        union all
        select * from {{ref("fact_aave_v3_bsc_ecosystem_incentives")}}
        union all
        select * from {{ref("fact_aave_v2_ethereum_ecosystem_incentives")}}
        union all
        select * from {{ref("fact_aave_v3_ethereum_ecosystem_incentives")}}
        union all
        select * from {{ref("fact_aave_v3_gnosis_ecosystem_incentives")}}
        union all
        select * from {{ref("fact_aave_v3_optimism_ecosystem_incentives")}}
        union all
        select * from {{ref("fact_aave_v2_polygon_ecosystem_incentives")}}
        union all
        select * from {{ref("fact_aave_v3_polygon_ecosystem_incentives")}}
    )
    
    , aave_ecosystem_incentives as (
        select 
            date
            , chain
            , token_address
            , sum(amount_usd) as ecosystem_incentives
            , sum(amount_nominal) as ecosystem_incentives_nominal
        from ecosystem_incentives
        group by date, chain, token_address
    )
    , aave_treasury as (
        select * from {{ref("fact_aave_aavura_treasury")}}
        union all
        select * from {{ref("fact_aave_v2_collector")}}
        union all
        select * from {{ref("fact_aave_safety_module")}}
        union all
        select * from {{ref("fact_aave_ecosystem_reserve")}}
    )
    , treasury as (
        select
            date
            , chain
            , token_address
            , sum(amount_usd) as treasury_value
            , sum(amount_nominal) as treasury_value_nominal
            , sum(case when token_address = lower('0x7Fc66500c84A76Ad7e9c93437bFc5Ac33E2DDaE9') then amount_usd else 0 end) as treasury_value_native
            , sum(case when token_address = lower('0x7Fc66500c84A76Ad7e9c93437bFc5Ac33E2DDaE9') then amount_nominal else 0 end) as treasury_value_native_nominal
        from aave_treasury
        group by date, chain, token_address
    )
    , aave_net_treasury as (
        select * from {{ref("fact_aave_v2_collector")}}
        union all
        select * from {{ref("fact_aave_aavura_treasury")}}
    )
    , net_treasury_data as (
        select
            date
            , chain
            , token_address
            , sum(amount_usd) as net_treasury_value
            , sum(amount_nominal) as net_treasury_value_nominal
        from aave_net_treasury
        group by 1, 2, 3
    )
    , dao_trading_revenue as (
        select
            date
            , chain
            , token_address
            , sum(trading_fees_usd) as trading_fees
            , sum(trading_fees_nominal) as trading_fees_nominal
        from {{ ref("fact_aave_dao_balancer_trading_fees")}}
        group by 1, 2, 3
    )
    , safety_incentives as (
        select
            date
            , chain
            , token_address
            , sum(amount_usd) as safety_incentives
            , sum(amount_nominal) as safety_incentives_nominal
        from {{ ref("fact_aave_dao_safety_incentives")}}
        group by 1, 2, 3
    )
    , gho_treasury_revenue as (
        select
            date
            , chain
            , token_address
            , sum(amount_usd) as gho_revenue
            , sum(amount_nominal) as gho_revenue_nominal
        from {{ ref("fact_aave_gho_treasury_revenue")}}
        group by 1, 2, 3
    )
select
    aave_outstanding_supply_net_deposits_deposit_revenue.date
    , chain
    , token_address

    , coalesce(interest_rate_fees_nominal, 0) as interest_rate_fees_nominal
    , coalesce(flashloan_fees_nominal, 0) as flashloan_fees_nominal
    , gho_revenue_nominal as gho_fees_nominal

    , coalesce(interest_rate_fees_nominal, 0) + coalesce(flashloan_fees_nominal, 0) + coalesce(gho_fees_nominal, 0) as fees_nominal
    , coalesce(interest_rate_fees, 0) + coalesce(flashloan_fees, 0) + coalesce(gho_revenue, 0) as fees

    , supply_side_deposit_revenue_nominal
    , supply_side_deposit_revenue

    , coalesce(supply_side_deposit_revenue_nominal, 0) as primary_supply_side_revenue_nominal
    , coalesce(supply_side_deposit_revenue, 0) as primary_supply_side_revenue

    , flashloan_fees_nominal as flashloan_supply_side_revenue_nominal
    , flashloan_fees as flashloan_supply_side_revenue

    , liquidation_revenue_nominal as liquidation_supply_side_revenue_nominal
    , liquidation_revenue as liquidation_supply_side_revenue

    , ecosystem_incentives_nominal as ecosystem_supply_side_revenue_nominal
    , ecosystem_incentives as ecosystem_supply_side_revenue

    , coalesce(flashloan_fees_nominal, 0) + coalesce(gho_revenue_nominal, 0) + coalesce(liquidation_revenue_nominal, 0) + coalesce(ecosystem_incentives_nominal, 0) as secondary_supply_side_revenue_nominal
    , coalesce(flashloan_fees, 0) + coalesce(gho_revenue, 0) + coalesce(liquidation_revenue, 0) + coalesce(ecosystem_incentives, 0) as secondary_supply_side_revenue

    , primary_supply_side_revenue_nominal + secondary_supply_side_revenue_nominal as total_supply_side_revenue_nominal
    , primary_supply_side_revenue + secondary_supply_side_revenue as total_supply_side_revenue

    , reserve_factor_revenue_nominal as reserve_factor_revenue_nominal
    , reserve_factor_revenue

    , trading_fees_nominal as dao_trading_revenue_nominal
    , trading_fees as dao_trading_revenue

    , gho_revenue_nominal
    , gho_revenue

    , coalesce(reserve_factor_revenue_nominal, 0) + coalesce(dao_trading_revenue_nominal, 0) + coalesce(gho_revenue_nominal, 0) as protocol_revenue_nominal
    , coalesce(reserve_factor_revenue, 0) + coalesce(dao_trading_revenue, 0) + coalesce(gho_revenue, 0) as protocol_revenue

    , ecosystem_incentives_nominal as ecosystem_incentives_nominal
    , ecosystem_incentives

    , safety_incentives_nominal as safety_incentives_nominal
    , safety_incentives

    , coalesce(ecosystem_incentives_nominal, 0) + coalesce(safety_incentives_nominal, 0) as token_incentives_nominal
    , coalesce(ecosystem_incentives, 0) + coalesce(safety_incentives, 0) as token_incentives

    , token_incentives_nominal as total_expenses_nominal
    , token_incentives as total_expenses 

    , coalesce(protocol_revenue_nominal, 0) - coalesce(total_expenses_nominal, 0) as earnings_nominal
    , coalesce(protocol_revenue, 0) - coalesce(total_expenses, 0) as earnings

    , outstanding_supply_nominal
    , outstanding_supply

    , net_deposits_nominal
    , net_deposits

    , tvl_nominal

    , treasury_value_nominal
    , treasury_value

    , net_treasury_value_nominal
    , net_treasury_value

    , treasury_value_native_nominal
    , treasury_value_native

    -- Standardized metrics
    , coalesce(interest_rate_fees_nominal, 0) as interest_rate_fees_native
    , coalesce(interest_rate_fees, 0) as interest_rate_fees
    , coalesce(flashloan_fees_nominal, 0) as flashloan_fees_native
    , flashloan_fees
    , gho_revenue_nominal as gho_fees_native
    , gho_revenue as gho_fees

    , coalesce(interest_rate_fees, 0) + coalesce(flashloan_fees, 0) + coalesce(gho_fees, 0) as ecosystem_revenue
    , coalesce(interest_rate_fees_nominal, 0) + coalesce(flashloan_fees_nominal, 0) + coalesce(gho_fees_nominal, 0) as ecosystem_revenue_native

    , supply_side_deposit_revenue + flashloan_fees as service_cash_flow
    , supply_side_deposit_revenue_nominal + flashloan_fees_nominal as service_cash_flow_native
    
    , liquidation_revenue as liquidator_cash_flow
    , liquidation_revenue_nominal as liquidator_cash_flow_native

    , reserve_factor_revenue as reserve_factor_treasury_cash_flow
    , reserve_factor_revenue_nominal as reserve_factor_treasury_cash_flow_native
    , dao_trading_revenue as dao_treasury_cash_flow
    , dao_trading_revenue_nominal as dao_treasury_cash_flow_native
    , gho_revenue as gho_treasury_cash_flow
    , gho_revenue_nominal as gho_treasury_cash_flow_native
    , coalesce(reserve_factor_revenue, 0) + coalesce(dao_trading_revenue, 0) + coalesce(gho_revenue, 0) as treasury_cash_flow
    , coalesce(reserve_factor_revenue_nominal, 0) + coalesce(dao_trading_revenue_nominal, 0) + coalesce(gho_revenue_nominal, 0) as treasury_cash_flow_native
    
    , outstanding_supply as lending_loans
    , outstanding_supply_nominal as lending_loans_native
    , net_deposits as lending_deposits
    , net_deposits_nominal as lending_deposits_native
    , tvl
    , tvl_nominal as tvl_native

    , treasury_value as treasury
    , treasury_value_native as treasury_native
    
    , net_treasury_value as net_treasury
    , net_treasury_value_nominal as net_treasury_native

from aave_outstanding_supply_net_deposits_deposit_revenue
left join aave_flashloan_fees using (date, chain, token_address)
left join aave_liquidation_supply_side_revenue using (date, chain, token_address)
left join aave_ecosystem_incentives using (date, chain, token_address)
left join dao_trading_revenue using (date, chain, token_address)
left join safety_incentives using (date, chain, token_address)
left join gho_treasury_revenue using (date, chain, token_address)
left join treasury using (date, chain, token_address)
left join net_treasury_data using (date, chain, token_address)
where aave_outstanding_supply_net_deposits_deposit_revenue.date < to_date(sysdate())