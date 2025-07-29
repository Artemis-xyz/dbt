
{{ 
    config(
        materialized='incremental',
        snowflake_warehouse='MAPLE',
        database='MAPLE',
        schema='core',
        alias='ez_metrics',
        incremental_strategy="merge",
        unique_key="date",
        on_schema_change="append_new_columns",
        merge_update_columns=var("backfill_columns", []),
        merge_exclude_columns=["created_on"] if not var("backfill_columns", []) else none,
        full_refresh=false,
        tags=["ez_metrics"],
    )
}}

{% set backfill_date = var("backfill_date", None) %}

WITH date_spine AS (
    SELECT date
    FROM {{ ref('dim_date_spine') }}
    WHERE date >= '2024-11-13' AND date < TO_DATE(SYSDATE())
)
, fees AS (
    SELECT
        date,
        SUM(net_interest_usd) AS interest_fees,
        SUM(platform_fees_usd) AS platform_fees,
        SUM(delegate_fees_usd) AS delegate_fees
    FROM {{ ref('fact_maple_fees') }}
    GROUP BY 1
)
, revenues AS (
    SELECT
        date,
        SUM(revenue) AS revenue
    FROM {{ ref('fact_maple_revenue') }}
    GROUP BY 1
)
, token_incentives AS (
    SELECT
        date
        , sum(incentive_native) as token_incentives_native
        , sum(incentive_usd) as token_incentives
    FROM {{ ref('fact_maple_token_incentives') }}
    GROUP BY 1
)
, tvl AS (
    SELECT
        date,
        SUM(tvl) AS tvl,
        SUM(outstanding_supply) AS outstanding_supply
    FROM {{ ref('fact_maple_agg_tvl') }}
    GROUP BY 1
)
, treasury AS (
    SELECT
        date,
        SUM(usd_balance) AS treasury, 
        SUM(native_balance) AS treasury_native
    FROM {{ ref('fact_maple_treasury') }}
    GROUP BY 1
)
, net_treasury AS (
    SELECT
        date,
        SUM(usd_balance) AS net_treasury,
        SUM(native_balance) AS net_treasury_native
    FROM {{ ref('fact_maple_treasury') }}   
    WHERE token <> 'SYRUP'
    GROUP BY 1
)
, treasury_native AS (
    SELECT
        date,
        SUM(native_balance) AS own_token_treasury_native,
        SUM(usd_balance) AS own_token_treasury
    FROM {{ ref('fact_maple_treasury') }}
    WHERE token = 'SYRUP'
    GROUP BY 1
)
, market_data AS (
    {{ get_coingecko_metrics('syrup')}}
)
, tokenholders AS (
    SELECT * FROM {{ ref('fact_maple_tokenholder_count')}}
)
, supply_data AS (
    SELECT * FROM {{ ref('fact_maple_supply')}}
)

SELECT 
    date_spine.date
    , 'maple' AS artemis_id

    -- Market Data
    , market_data.price
    , market_data.market_cap
    , market_data.fdmc
    , market_data.token_volume

    -- Usage Data
    , tokenholders.token_holder_count
    , tvl.outstanding_supply AS lending_loans
    , tvl.outstanding_supply + tvl.tvl AS lending_loan_capacity

    -- Fee Data
    , fees.interest_fees
    , fees.platform_fees
    , fees.delegate_fees
    , fees.interest_fees + fees.platform_fees + fees.delegate_fees AS fees
    , fees.interest_fees AS staking_fee_allocation
    , fees.platform_fees AS treasury_fee_allocation
    , 0.33 * fees.delegate_fees AS other_fee_allocation
        -- 33% of delegate fees goes to the pool delegates
    , 0.66 * fees.delegate_fees AS dao_fee_allocation
    , COALESCE(supply_data.buybacks, 0) as buybacks
    
    -- Fees
    , COALESCE(revenues.revenue, 0) + COALESCE(supply_data.buybacks, 0) + COALESCE(dao_fee_allocation, 0) AS revenue
    , COALESCE(token_incentives.token_incentives, 0) AS token_incentives
    , COALESCE(token_incentives.token_incentives, 0) AS total_expenses
    , COALESCE(revenue, 0) - COALESCE(token_incentives.token_incentives, 0) AS earnings

    -- Treasury Data
    , treasury.treasury
    , net_treasury.net_treasury
    , treasury_native.own_token_treasury

    --Supply Data
    , COALESCE(supply_data.premine_unlocks_native, 0) AS premine_unlocks_native
    , COALESCE(supply_data.emissions_native, 0) AS gross_emissions_native
    , COALESCE(supply_data.emissions_native, 0) * COALESCE(price.price, 0) AS gross_emissions
    , COALESCE(supply_data.circulating_supply_native, 0) AS circulating_supply_native

    --Turnover Data
    , price.token_turnover_circulating
    , price.token_turnover_fdv
    
    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on
FROM date_spine
LEFT JOIN market_data USING(date)
LEFT JOIN fees USING(date)
LEFT JOIN revenues USING(date)
LEFT JOIN token_incentives USING(date)
LEFT JOIN tvl USING(date)
LEFT JOIN treasury USING(date)
LEFT JOIN treasury_native USING(date)
LEFT JOIN net_treasury USING(date)
LEFT JOIN tokenholders USING(date)
LEFT JOIN supply_data USING(date)
where true
{{ ez_metrics_incremental('date_spine.date', backfill_date) }}
and date_spine.date < to_date(sysdate())