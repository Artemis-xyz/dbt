{{
    config(
        materialized="table",
        snowflake_warehouse="AAVE",
        database="aave",
        schema="core",
        alias="ez_metrics_by_chain",
    )
}}

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
            , sum(borrows_usd) as outstanding_supply
            , sum(supply_usd) as net_deposits
            , net_deposits - outstanding_supply as tvl
            , sum(deposit_revenue) as supply_side_deposit_revenue
            , sum(interest_rate_fees) as interest_rate_fees
            , sum(reserve_factor_revenue) as reserve_factor_revenue
        from deposits_borrows_lender_revenue
        group by 1, 2
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
            , sum(amount_usd) as flashloan_fees
        from flashloan_fees
        group by 1, 2
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
            , sum(liquidation_revenue) as liquidation_revenue
        from liquidation_revenue
        group by 1, 2
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
            , sum(case when token_address = lower('0x7Fc66500c84A76Ad7e9c93437bFc5Ac33E2DDaE9') then amount_usd else 0 end) as treasury_value_native
            , sum(amount_usd) as treasury_value
        from aave_treasury
        group by date, 2
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
            , sum(amount_usd) as net_treasury_value
        from aave_net_treasury
        group by 1, 2
    )
    , aave_ecosystem_incentives as (
        select 
            date
            , chain
            , sum(amount_usd) as ecosystem_incentives
        from ecosystem_incentives
        group by 1, 2
    )
    , dao_trading_revenue as (
        select
            date
            , chain
            , sum(trading_fees_usd) as trading_fees
        from {{ ref("fact_aave_dao_balancer_trading_fees")}}
        group by 1, 2
    )
    , safety_incentives as (
        select
            date
            , chain
            , sum(amount_usd) as safety_incentives
        from {{ ref("fact_aave_dao_safety_incentives")}}
        group by 1, 2
    )
    , gho_treasury_revenue as (
        select
            date
            , chain
            , sum(amount_usd) as gho_revenue
        from {{ ref("fact_aave_gho_treasury_revenue")}}
        group by 1, 2
    )
   
select
    aave_outstanding_supply_net_deposits_deposit_revenue.date
    , chain
    , supply_side_deposit_revenue
    , coalesce(supply_side_deposit_revenue, 0) as primary_supply_side_revenue
    , flashloan_fees as flashloan_supply_side_revenue
    , liquidation_revenue as liquidation_supply_side_revenue
    , ecosystem_incentives as ecosystem_supply_side_revenue
    , coalesce(flashloan_fees, 0) + coalesce(gho_revenue, 0) + coalesce(liquidation_revenue, 0) + coalesce(ecosystem_incentives, 0) as secondary_supply_side_revenue
    , primary_supply_side_revenue + secondary_supply_side_revenue as total_supply_side_revenue
    , trading_fees as dao_trading_revenue
    , gho_revenue
    , coalesce(reserve_factor_revenue, 0) as reserve_factor_revenue
    , coalesce(reserve_factor_revenue, 0) + coalesce(dao_trading_revenue, 0) + coalesce(gho_revenue, 0) as protocol_revenue
    , ecosystem_incentives
    , safety_incentives
    , coalesce(ecosystem_incentives, 0) + coalesce(safety_incentives, 0) as token_incentives
    , token_incentives as total_expenses 
    , coalesce(protocol_revenue, 0) - coalesce(total_expenses, 0) as earnings
    , outstanding_supply
    , net_deposits
    , treasury_value
    , net_treasury_value
    , treasury_value_native


    -- Standardized metrics
    , interest_rate_fees as interest_rate_fees
    , flashloan_fees
    , gho_revenue as gho_fees
    , coalesce(interest_rate_fees, 0) + coalesce(flashloan_fees, 0) + coalesce(gho_fees, 0) as fees

    , supply_side_deposit_revenue + flashloan_fees as service_fee_allocation
    , liquidation_revenue as liquidator_fee_allocation
    , reserve_factor_revenue as reserve_factor_treasury_fee_allocation
    , dao_trading_revenue as dao_treasury_fee_allocation
    , gho_revenue as gho_treasury_fee_allocation
    , coalesce(reserve_factor_revenue, 0) + coalesce(dao_trading_revenue, 0) + coalesce(gho_revenue, 0) as treasury_fee_allocation
    
    , outstanding_supply as lending_loans
    , net_deposits as lending_deposits
    , tvl

    , treasury_value as treasury
    , treasury_value_native as treasury_native
    , net_treasury_value as net_treasury
from aave_outstanding_supply_net_deposits_deposit_revenue
left join aave_flashloan_fees using (date, chain)
left join aave_liquidation_supply_side_revenue using (date, chain)
left join aave_ecosystem_incentives using (date, chain)
left join dao_trading_revenue using (date, chain)
left join safety_incentives using (date, chain)
left join gho_treasury_revenue using (date, chain)
left join treasury using (date, chain)
left join net_treasury_data using (date, chain)
where aave_outstanding_supply_net_deposits_deposit_revenue.date < to_date(sysdate())