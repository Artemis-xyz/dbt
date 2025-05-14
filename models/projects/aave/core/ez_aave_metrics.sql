{{
    config(
        materialized="table",
        snowflake_warehouse="AAVE",
        database="aave",
        schema="core",
        alias="ez_metrics",
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
            , sum(borrows_usd) as outstanding_supply
            , sum(supply_usd) as net_deposits
            , net_deposits - outstanding_supply as tvl
            , sum(deposit_revenue) as supply_side_deposit_revenue
            , sum(interest_rate_fees) as interest_rate_fees
            , sum(reserve_factor_revenue) as reserve_factor_revenue
        from deposits_borrows_lender_revenue
        group by 1
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
            , sum(amount_usd) as flashloan_fees
        from flashloan_fees
        group by 1
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
            , sum(liquidation_revenue) as liquidation_revenue
        from liquidation_revenue
        group by 1
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
            , sum(case when token_address = lower('0x7Fc66500c84A76Ad7e9c93437bFc5Ac33E2DDaE9') then amount_usd else 0 end) as treasury_value_native
            , sum(amount_usd) as treasury_value
        from aave_treasury
        group by date
    )
    , aave_net_treasury as (
        select * from {{ref("fact_aave_v2_collector")}}
        union all
        select * from {{ref("fact_aave_aavura_treasury")}}
    )
    , net_treasury_data as (
        select
            date
            , sum(amount_usd) as net_treasury_value
        from aave_net_treasury
        group by 1
    )
    , aave_ecosystem_incentives as (
        select 
            date
            , sum(amount_usd) as ecosystem_incentives
        from ecosystem_incentives
        group by 1
    )
    , dao_trading_revenue as (
        select
            date
            , sum(trading_fees_usd) as trading_fees
        from {{ ref("fact_aave_dao_balancer_trading_fees")}}
        group by 1
    )
    , safety_incentives as (
        select
            date
            , sum(amount_usd) as safety_incentives
        from {{ ref("fact_aave_dao_safety_incentives")}}
        group by 1
    )
    , gho_treasury_revenue as (
        select
            date
            , sum(amount_usd) as gho_revenue
        from {{ ref("fact_aave_gho_treasury_revenue")}}
        group by 1
    )
    , aave_token_holders as (
        select
            date
            , token_holder_count
        from {{ ref("fact_aave_token_holders")}}
    )
    , coingecko_metrics as (
        select 
            date
            , shifted_token_price_usd as price
            , shifted_token_h24_volume_usd as h24_volume
            , shifted_token_market_cap as market_cap
            , t2.total_supply * price as fdmc
            , shifted_token_h24_volume_usd / market_cap as token_turnover_circulating
            , shifted_token_h24_volume_usd / fdmc as token_turnover_fdv
        from {{ ref("fact_coingecko_token_date_adjusted_gold") }} t1
        inner join
            (
                select
                    token_id, coalesce(token_max_supply, token_total_supply) as total_supply
                from {{ ref("fact_coingecko_token_realtime_data") }}
                where token_id = 'aave'
            ) t2
            on t1.coingecko_id = t2.token_id
        where
            coingecko_id = 'aave'
            and date < dateadd(day, -1, to_date(sysdate()))
    )
select
    aave_outstanding_supply_net_deposits_deposit_revenue.date
    , coalesce(interest_rate_fees, 0) + coalesce(flashloan_fees, 0) + coalesce(gho_revenue, 0) as fees
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
    
    -- Currently don't take into account incentives
    , ecosystem_incentives
    , safety_incentives
    , coalesce(ecosystem_incentives, 0) + coalesce(safety_incentives, 0) as token_incentives

    , token_incentives as total_expenses 
    , coalesce(protocol_revenue, 0) - coalesce(total_expenses, 0) as protocol_earnings
    , outstanding_supply
    , net_deposits
    
    , treasury_value
    , treasury_value_native
    , net_treasury_value
    , token_holder_count
    , h24_volume
    -- Standardized metrics
    , interest_rate_fees as interest_rate_fees
    , flashloan_fees
    , gho_revenue as gho_fees
    , coalesce(interest_rate_fees, 0) + coalesce(flashloan_fees, 0) + coalesce(gho_fees, 0) as ecosystem_revenue

    , supply_side_deposit_revenue + flashloan_fees as service_cash_flow
    , liquidation_revenue as liquidator_cash_flow
    
    , reserve_factor_revenue as reserve_factor_treasury_cash_flow
    , dao_trading_revenue as dao_treasury_cash_flow
    , gho_revenue as gho_treasury_cash_flow
    , coalesce(reserve_factor_revenue, 0) + coalesce(dao_trading_revenue, 0) + coalesce(gho_revenue, 0) as treasury_cash_flow
    
    , outstanding_supply as lending_loans
    , net_deposits as lending_deposits
    , tvl

    , treasury_value as treasury
    , treasury_value_native as treasury_native
    , net_treasury_value as net_treasury

    , h24_volume as token_volume
    , price
    , market_cap
    , fdmc
    , token_turnover_circulating
    , token_turnover_fdv
from aave_outstanding_supply_net_deposits_deposit_revenue
left join aave_flashloan_fees using (date)
left join aave_liquidation_supply_side_revenue using (date)
left join aave_ecosystem_incentives using (date)
left join dao_trading_revenue using (date)
left join safety_incentives using (date)
left join gho_treasury_revenue using (date)
left join treasury using (date)
left join net_treasury_data using (date)
left join aave_token_holders using (date)
left join coingecko_metrics using (date)
where aave_outstanding_supply_net_deposits_deposit_revenue.date < to_date(sysdate())