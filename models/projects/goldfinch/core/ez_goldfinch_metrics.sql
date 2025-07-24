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
        , merge_exclude_columns=["created_on"] | reject('in', var("backfill_columns", [])) | list
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
    , m.interest_fees
    , m.withdrawal_revenue as withdrawal_fees
    , coalesce(m.interest_fees,0) + coalesce(m.withdrawal_revenue,0) as fees
    , m.supply_side_fees as primary_supply_side_revenue
    , 0 as secondary_supply_side_revenue
    , primary_supply_side_revenue + secondary_supply_side_revenue as total_supply_side_revenue
    , m.interest_revenue
    , m.withdrawal_revenue
    , coalesce(m.interest_revenue,0) + coalesce(m.withdrawal_revenue,0) as revenue
    , coalesce(ti.token_incentives,0) as token_incentives
    , 0 as operating_expenses
    , coalesce(ti.token_incentives,0) + coalesce(operating_expenses,0) as total_expenses
    , coalesce(revenue,0) - coalesce(total_expenses,0) as earnings
    , m.net_deposits as net_deposits
    , 0 as outstanding_supply
    , t.treasury as treasury_value
    , t.treasury_native as treasury_value_native
    , t.net_treasury as net_treasury_value
    , {{ daily_pct_change('m.tvl') }} as tvl_growth
    , th.token_holder_count
    -- Standardized Metrics
    -- Token Metrics
    , coalesce(pd.price,0) as price
    , coalesce(pd.market_cap,0) as market_cap
    , coalesce(pd.fdmc,0) as fdmc
    , coalesce(pd.token_volume,0) as token_volume
    -- Lending Metrics
    , coalesce(m.net_deposits,0) as lending_deposits
    , coalesce(m.tvl,0) as lending_loan_capacity
    , coalesce(m.interest_revenue,0) as lending_interest_fees
    -- Crypto Metrics
    , coalesce(m.tvl,0) as tvl
    , coalesce(m.tvl,0) - LAG(coalesce(m.tvl,0)) OVER (ORDER BY date) as tvl_net_change
    -- Cash Flow
    , coalesce(m.interest_fees,0) + coalesce(m.withdrawal_revenue,0) as ecosystem_revenue
    , coalesce(m.supply_side_fees,0) as service_fee_allocation
    , coalesce(m.interest_revenue,0) + coalesce(m.withdrawal_revenue,0) as token_fee_allocation 
        -- This is cashflow to the DAO-controlled treasury
    -- Protocol Metrics
    , coalesce(t.treasury,0) as treasury
    , coalesce(t.treasury_native,0) as treasury_native
    , coalesce(t.net_treasury,0) as net_treasury
    , coalesce(t.net_treasury_native,0) as net_treasury_native
    , coalesce(t.own_token_treasury,0) as own_token_treasury
    , coalesce(t.own_token_treasury_native,0) as own_token_treasury_native
    -- Turnover Metrics
    , coalesce(pd.token_turnover_circulating,0) as token_turnover_circulating
    , coalesce(pd.token_turnover_fdv,0) as token_turnover_fdv
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
