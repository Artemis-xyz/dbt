{{
    config(
        materialized="incremental"
        , snowflake_warehouse="GOLDFINCH"
        , database="goldfinch"
        , schema="core"
        , alias="ez_metrics"
        , incremental_strategy="merge"
        , unique_key="date"
        , on_schema_change="append_new_columns"
        , merge_update_columns=var("backfill_columns", [])
        , merge_exclude_columns=["created_on"] if not var("backfill_columns", []) else none
        , full_refresh=false
        , tags=["ez_metrics"]
    )
}}

{% set backfill_date = var("backfill_date", None) %}

with fees_tvl_metrics as(
    SELECT
        date,
        SUM(interest_fees) as interest_fees, -- Total interest fees paid by borrowers
        SUM(supply_side_fees) as supply_side_fees, -- Net interest received by depositors
        SUM(interest_revenue) as interest_revenue, -- Interest revenue only
        SUM(withdrawal_revenue) as withdrawal_revenue, -- Withdrawal revenue only
        AVG(net_deposits) as net_deposits,
        AVG(tvl) as tvl
    FROM {{ ref('fact_goldfinch_metrics') }}
    GROUP BY date
)
, token_incentives_cte as (
    SELECT
        date,
        SUM(amount_usd) as token_incentives
    FROM {{ ref('fact_goldfinch_token_incentives') }}
    GROUP BY date
)
, treasury as (
    select 
        date
        , sum(treasury) as treasury
        , sum(treasury_native) as treasury_native
        , sum(net_treasury) as net_treasury
        , sum(net_treasury_native) as net_treasury_native
        , sum(own_token_treasury) as own_token_treasury
        , sum(own_token_treasury_native) as own_token_treasury_native
    from {{ ref('ez_goldfinch_metrics_by_token') }}
    group by 1
)
, price_data as ({{ get_coingecko_metrics("goldfinch") }})
, token_holder_data as (
    select
        date
        , token_holder_count
    from {{ ref("fact_goldfinch_tokenholders")}}
)

SELECT
    m.date
    , 'goldfinch' as artemis_id
    
    -- Standardized Metrics

    -- Market Data
    , pd.price
    , pd.market_cap
    , pd.fdmc
    , pd.token_volume

    -- Usage Data
    , m.net_deposits as lending_deposits
    , m.tvl as lending_loans
    , m.tvl as lending_loan_capacity
    , m.tvl as lending_tvl
    , m.tvl as tvl
    , th.token_holder_count

    -- Fee Data
    , m.interest_fees
    , m.withdrawal_revenue as withdrawal_fees
    , coalesce(m.interest_fees,0) + coalesce(m.withdrawal_revenue,0) as fees
    , m.supply_side_fees as lp_fee_allocation
    , coalesce(m.interest_revenue,0) + coalesce(m.withdrawal_revenue,0) as tokenholder_fee_allocation 
        -- This is cashflow to the DAO-controlled treasury
    
    -- Financial Statements
    , coalesce(m.interest_revenue,0) + coalesce(m.withdrawal_revenue,0) as revenue
    , coalesce(ti.token_incentives,0) as token_incentives
    , coalesce(revenue,0) - coalesce(token_incentives,0) as earnings

    -- Treasury Data
    , t.treasury
    , t.net_treasury
    , t.own_token_treasury

    -- Turnover Metrics
    , pd.token_turnover_circulating
    , pd.token_turnover_fdv

    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on
FROM fees_tvl_metrics m
LEFT JOIN token_incentives_cte ti using (date)
LEFT JOIN treasury t using (date)
LEFT JOIN token_holder_data th using (date)
LEFT JOIN price_data pd using (date)
where true
{{ ez_metrics_incremental('m.date', backfill_date) }}
and m.date < to_date(sysdate())
