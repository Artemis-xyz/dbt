{{
    config(
        materialized='incremental',
        snowflake_warehouse='jito',
        database='jito',
        schema='core',
        alias='ez_metrics',
        incremental_strategy="merge",
        unique_key="date",
        on_schema_change="append_new_columns",
        merge_update_columns=var("backfill_columns", []),
        merge_exclude_columns=["created_on"] if not var("backfill_columns", []) else none,
        full_refresh=var("full_refresh", false),
        tags=["ez_metrics"],
    )
}}

{% set backfill_date = var("backfill_date", None) %}

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
            , premine_unlocks as premine_unlocks_native
            , 0 as emissions_native
            , 0 as burns_native
        FROM {{ ref('fact_jito_unlock_schedule') }}
    )
    , market_metrics as (
        {{get_coingecko_metrics('jito-governance-token')}}
    )

SELECT 
    date_spine.date
    , 'jito' as artemis_id

    --Standardized Metrics
    --Market Metrics
    , market_metrics.price as price
    , market_metrics.token_volume as token_volume
    , market_metrics.market_cap as market_cap
    , market_metrics.fdmc as fdmc

    -- Usage Metrics
    , tip_txns as block_infra_txns
    , tip_dau as block_infra_dau
    , tvl as lst_tvl
    , tvl as tvl

    -- Fee Metrics
    , withdraw_management_fees as lst_fees
    , tip_fees as block_infra_fees
    , coalesce(tip_fees, 0) + coalesce(withdraw_management_fees, 0) as fees
    , jito_dau_txns_fees_fee_allocation.equity_fee_allocation as foundation_fee_allocation
    , jito_dau_txns_fees_fee_allocation.treasury_fee_allocation as treasury_fee_allocation
    , jito_dau_txns_fees_fee_allocation.strategy_fee_allocation as other_fee_allocation
    , jito_dau_txns_fees_fee_allocation.validator_fee_allocation as validator_fee_allocation

    -- Financial Metrics
    , tip_revenue as revenue

    -- Token Turnover Metrics
    , market_metrics.token_turnover_circulating as token_turnover_circulating
    , market_metrics.token_turnover_fdv as token_turnover_fdv

    -- Supply Metrics
    , daily_supply_data.emissions_native as emissions_native
    , daily_supply_data.premine_unlocks_native as premine_unlocks_native
    , daily_supply_data.burns_native as burns_native
    , coalesce(daily_supply_data.emissions_native, 0) + coalesce(daily_supply_data.premine_unlocks_native, 0) - coalesce(daily_supply_data.burns_native, 0) as net_supply_change_native
    , sum(coalesce(daily_supply_data.emissions_native, 0) + coalesce(daily_supply_data.premine_unlocks_native, 0) - coalesce(daily_supply_data.burns_native, 0)) over (order by daily_supply_data.date) as circulating_supply_native

    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on
FROM date_spine
LEFT JOIN jito_mgmt_withdraw_fees using (date)
LEFT JOIN jito_dau_txns_fees_fee_allocation using (date)
LEFT JOIN jito_dau_txns_fees using (date)
LEFT JOIN jito_tvl using (date)
LEFT JOIN daily_supply_data using (date)
LEFT JOIN market_metrics using (date)
where true
{{ ez_metrics_incremental('date_spine.date', backfill_date) }}
and date_spine.date < to_date(sysdate())