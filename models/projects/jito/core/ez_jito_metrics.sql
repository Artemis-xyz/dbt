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
    , jito_dau_txns_fees as ( -- Tips
        SELECT 
            day as date
            , tip_fees
            , tip_revenue
            , tip_supply_side_fees
            , tip_txns
            , tip_dau
        FROM {{ ref('fact_jito_dau_txns_fees') }}
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
        {{get_coingecko_metrics('jto')}}
    )

SELECT 
    date

    --Old metrics needed for compatibility
    , coalesce(withdraw_management_fees, 0) as withdraw_management_fees
    , coalesce(tip_fees, 0) as tip
    , coalesce(tip_fees, 0) + coalesce(withdraw_management_fees, 0) as fees
    , coalesce(tip_revenue, 0) + coalesce(withdraw_management_fees, 0) as revenue
    , coalesce(tip_supply_side_fees, 0) as supply_side_fees
    , coalesce(tip_txns, 0) as txns
    , coalesce(tip_dau, 0) as dau
    , coalesce(tvl, 0) as tvl
    , coalesce(tvl, 0) as amount_staked_usd
    , coalesce(tvl_change, 0) as amount_staked_usd_net_change

    --Standardized Metrics
    , coalesce(withdraw_management_fees, 0) as withdraw_management_fees
    , coalesce(tip_fees, 0) as ecosystem_fees
    , coalesce(withdraw_management_fees, 0) + coalesce(tip_fees, 0) as gross_protocol_fees
    , coalesce(tip_revenue, 0) + coalesce(withdraw_management_fees, 0) as gross_protocol_revenue
    , coalesce(tip_supply_side_fees, 0) as ecosystem_revenue
    , coalesce(tip_txns, 0) as ecosystem_txns
    , coalesce(tip_dau, 0) as ecosystem_dau
    , coalesce(tvl, 0) as tvl
    , coalesce(tvl, 0) as tvl_native
    , coalesce(tvl_change, 0) as tvl_native_net_change

    --Market Metrics
    , coalesce(market_metrics.price, 0) as price
    , coalesce(market_metrics.token_volume, 0) as token_volume
    , coalesce(market_metrics.market_cap, 0) as market_cap
    , coalesce(market_metrics.fdmc, 0) as fdmc
    , coalesce(market_metrics.token_turnover_circulating, 0) as token_turnover_circulating
    , coalesce(market_metrics.token_turnover_fdv, 0) as token_turnover_fdv

FROM date_spine ds
LEFT JOIN jito_mgmt_withdraw_fees using (date)
LEFT JOIN jito_dau_txns_fees using (date)
LEFT JOIN jito_tvl using (date)
LEFT JOIN market_metrics using (date)