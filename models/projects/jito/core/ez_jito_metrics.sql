{{
    config(
        materialized='view',
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
    , jito_dau_txns_fees_cash_flow as ( -- Tips
        SELECT 
            day as date
            , sum(CASE WHEN day < '2025-03-07'
                    THEN tip_fees * 0.05
                    ELSE tip_fees * 0.03 -- 2.7% to DAO + 3% to Jito
                END) as equity_cash_flow
            , sum(CASE WHEN day < '2025-03-07'
                    THEN 0
                    ELSE tip_fees * 0.027 -- 2.7% to DAO + 3% to Jito
                END) as treasury_cash_flow
            , sum(CASE WHEN day < '2025-03-07'
                    THEN tip_fees * .95
                    ELSE tip_fees * 0.94 -- 94% to validators + 0.3% to SOL/JTO vault operators
                END) as validator_cash_flow
            , sum(CASE WHEN day < '2025-03-07'
                    THEN 0
                    ELSE tip_fees * 0.003 -- 94% to validators + 0.3% to SOL/JTO vault operators
                END) as strategy_cash_flow
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
            , sum(usd_balance) as tvl
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
    , market_metrics as (
        {{get_coingecko_metrics('jito-governance-token')}}
    )

SELECT 
    date

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

    -- Usage Metrics
    , coalesce(tip_txns, 0) as block_infra_txns
    , coalesce(tip_dau, 0) as block_infra_dau
    , coalesce(tvl, 0) as tvl
    , coalesce(tvl_change, 0) as tvl_net_change
    
    -- Cash Flow Metrics
    , coalesce(withdraw_management_fees, 0) as lst_fees
    , coalesce(tip_fees, 0) as block_infra_fees
    , coalesce(withdraw_management_fees, 0) + coalesce(tip_fees, 0) as gross_protocol_revenue
    , coalesce(jito_dau_txns_fees_cash_flow.equity_cash_flow, 0) as equity_cash_flow
    , coalesce(jito_dau_txns_fees_cash_flow.treasury_cash_flow, 0) as treasury_cash_flow
    , coalesce(jito_dau_txns_fees_cash_flow.strategy_cash_flow, 0) as strategy_cash_flow
    , coalesce(jito_dau_txns_fees_cash_flow.validator_cash_flow, 0) as validator_cash_flow

    --Market Metrics
    , coalesce(market_metrics.price, 0) as price
    , coalesce(market_metrics.token_volume, 0) as token_volume
    , coalesce(market_metrics.market_cap, 0) as market_cap
    , coalesce(market_metrics.fdmc, 0) as fdmc
    , coalesce(market_metrics.token_turnover_circulating, 0) as token_turnover_circulating
    , coalesce(market_metrics.token_turnover_fdv, 0) as token_turnover_fdv

FROM date_spine ds
LEFT JOIN jito_mgmt_withdraw_fees using (date)
LEFT JOIN jito_dau_txns_fees_cash_flow using (date)
LEFT JOIN jito_dau_txns_fees using (date)
LEFT JOIN jito_tvl using (date)
LEFT JOIN market_metrics using (date)