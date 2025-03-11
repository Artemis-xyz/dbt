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
        FROM {{ ref('fact_jitosol_tvl') }}
        GROUP BY 1
    )
    , date_spine as (
        SELECT
            date
        FROM {{ ref('dim_date_spine') }}
        WHERE date between (select min(date) from jito_dau_txns_fees) and (to_date(sysdate()))
    )

SELECT 
    date
    , withdraw_management_fees
    , tip_fees
    , withdraw_management_fees + tip_fees as fees
    , tip_revenue + tip_fees as revenue
    , tip_supply_side_fees as supply_side_fees
    , tip_txns as txns
    , tip_dau as dau
    , tvl
FROM date_spine ds
LEFT JOIN jito_mgmt_withdraw_fees using (date)
LEFT JOIN jito_dau_txns_fees using (date)
LEFT JOIN jito_tvl using (date)