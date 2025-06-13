{{
    config(
        materialized='table',
        snowflake_warehouse='jito',
        database='jito',
        schema='core',
        alias='ez_metrics'
    )
}}
with 
    jito_mgmt_withdraw_fees as (
        SELECT 
            date
            , withdraw_management_fees
        FROM {{ ref('fact_jito_mgmt_withdraw_fees') }}
    )
    , jito_dau_txns_fees_fee_allocation as ( -- Tips
        SELECT 
            day as date
            , sum(CASE WHEN day < '2025-03-07'
                    THEN tip_fees * 0.05
                    ELSE tip_fees * 0.03 -- 2.7% to DAO + 3% to Jito
                END) as equity_fee_allocation
            , sum(CASE WHEN day < '2025-03-07'
                    THEN 0
                    ELSE tip_fees * 0.027 -- 2.7% to DAO + 3% to Jito
                END) as treasury_fee_allocation
            , sum(CASE WHEN day < '2025-03-07'
                    THEN tip_fees * .95
                    ELSE tip_fees * 0.94 -- 94% to validators + 0.3% to SOL/JTO vault operators
                END) as validator_fee_allocation
            , sum(CASE WHEN day < '2025-03-07'
                    THEN 0
                    ELSE tip_fees * 0.003 -- 94% to validators + 0.3% to SOL/JTO vault operators
                END) as strategy_fee_allocation
        FROM {{ ref('fact_jito_dau_txns_fees')}}
        group by day
    )
    , jito_dau_txns_fees as ( -- Tips
        SELECT 
            day as date
            , tip_fees
            , tip_txns
            , tip_dau
            , tip_revenue
            , tip_supply_side_fees
        FROM {{ ref('fact_jito_dau_txns_fees')}}
    )
    , jito_tvl as (
        SELECT
            date
            , sum(balance) as tvl
            , tvl - lag(tvl) over (order by date) as tvl_change
        FROM {{ ref('fact_jitosol_tvl') }}
        GROUP BY 1
    )
    , date_spine as (
        SELECT
            date
        FROM {{ ref('dim_date_spine') }}
        WHERE date between (select min(date) from jito_dau_txns_fees) and (to_date(sysdate()))
    )
    , daily_supply_data as (
        SELECT 
            date
            , 0 as emissions_native
            , pre_mine_unlocks as premine_unlocks_native
            , 0 as burns_native
        FROM {{ ref('fact_jito_daily_premine_unlocks') }}
    )
    , market_metrics as (
        {{get_coingecko_metrics('jito-governance-token')}}
    )

SELECT 
    date_spine.date
    , 'jito' as app
    , 'DeFi' as category

    --Old metrics needed for compatibility
    , coalesce(withdraw_management_fees, 0) as withdraw_management_fees
    , coalesce(tip_fees, 0) as tip_fees
    , coalesce(tip_fees, 0) + coalesce(withdraw_management_fees, 0) as fees
    , coalesce(tip_revenue, 0) + coalesce(withdraw_management_fees, 0) as revenue
    , coalesce(tip_supply_side_fees, 0) as supply_side_fees
    , coalesce(tip_txns, 0) as txns
    , coalesce(tip_dau, 0) as dau
    , coalesce(tvl, 0) as amount_staked_usd
    , coalesce(tvl_change, 0) as amount_staked_usd_net_change

    --Standardized Metrics

    --Market Metrics
    , coalesce(market_metrics.price, 0) as price
    , coalesce(market_metrics.token_volume, 0) as token_volume
    , coalesce(market_metrics.market_cap, 0) as market_cap
    , coalesce(market_metrics.fdmc, 0) as fdmc

    -- Usage Metrics
    , coalesce(tip_txns, 0) as block_infra_txns
    , coalesce(tip_dau, 0) as block_infra_dau
    , coalesce(tvl, 0) as tvl
    , coalesce(tvl_change, 0) as tvl_net_change

    -- Cashflow Metrics
    , coalesce(withdraw_management_fees, 0) as lst_fees
    , coalesce(tip_fees, 0) as block_infra_fees
    , coalesce(withdraw_management_fees, 0) + coalesce(tip_fees, 0) as ecosystem_revenue
    , coalesce(jito_dau_txns_fees_fee_allocation.equity_fee_allocation, 0) as equity_fee_allocation
    , coalesce(jito_dau_txns_fees_fee_allocation.treasury_fee_allocation, 0) as treasury_fee_allocation
    , coalesce(jito_dau_txns_fees_fee_allocation.strategy_fee_allocation, 0) as strategy_fee_allocation
    , coalesce(jito_dau_txns_fees_fee_allocation.validator_fee_allocation, 0) as validator_fee_allocation

    -- Token Turnover Metrics
    , coalesce(market_metrics.token_turnover_circulating, 0) as token_turnover_circulating
    , coalesce(market_metrics.token_turnover_fdv, 0) as token_turnover_fdv

    -- JTO Token Supply Data
    , coalesce(daily_supply_data.emissions_native, 0) as emissions_native
    , coalesce(daily_supply_data.premine_unlocks_native, 0) as premine_unlocks_native
    , coalesce(daily_supply_data.burns_native, 0) as burns_native
    , coalesce(daily_supply_data.emissions_native, 0) + coalesce(daily_supply_data.premine_unlocks_native, 0) - coalesce(daily_supply_data.burns_native, 0) as net_supply_change_native
    , sum(coalesce(daily_supply_data.emissions_native, 0) + coalesce(daily_supply_data.premine_unlocks_native, 0) - coalesce(daily_supply_data.burns_native, 0)) over (order by daily_supply_data.date) as circulating_supply_native

FROM date_spine
LEFT JOIN jito_mgmt_withdraw_fees using (date)
LEFT JOIN jito_dau_txns_fees_fee_allocation using (date)
LEFT JOIN jito_dau_txns_fees using (date)
LEFT JOIN jito_tvl using (date)
LEFT JOIN daily_supply_data using (date)
LEFT JOIN market_metrics using (date)
WHERE date_spine.date < to_date(sysdate())