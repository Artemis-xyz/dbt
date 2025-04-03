{{
    config(
        materialized="table"
        , snowflake_warehouse="GOLDFINCH"
        , database="goldfinch"
        , schema="core"
        , alias="ez_metrics"
    )
}}

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
    , coalesce(revenue,0) - coalesce(total_expenses,0) as protocol_earnings
    , m.net_deposits as net_deposits
    , 0 as outstanding_supply
    , t.treasury_value as treasury_value
    , tn.treasury_native_value as treasury_value_native
    , nt.net_treasury_value as net_treasury_value
    , {{ daily_pct_change('m.tvl') }} as tvl_growth
    , th.token_holder_count

    -- Lending Metrics
    , m.net_deposits as lending_deposits
    , m.tvl as lending_loan_capacity

    -- Crypto Metrics
    , m.tvl as tvl
    , {{ daily_pct_change('m.tvl') }} as tvl_net_change

    -- Cash Flow
    , coalesce(m.interest_revenue,0) + coalesce(m.withdrawal_revenue,0) as gross_protocol_revenue
    , m.supply_side_fees as service_cash_flow
    , coalesce(ti.token_incentives,0) as token_cash_flow
    , coalesce(m.interest_revenue,0) + coalesce(m.withdrawal_revenue,0) - coalesce(ti.token_incentives,0) as foundation_cash_flow
    
    -- Protocol Metrics
    , t.treasury_value as treasury
    , tn.treasury_native_value as treasury_native
    , tn.treasury_native_value - LAG(tn.treasury_native_value) 
        OVER (ORDER BY date) AS treasury_native_net_change

    -- Token Metrics
    , pd.price
    , pd.market_cap
    , pd.fdmc
    , pd.token_volume

    -- Turnover Metrics
    , pd.token_turnover_circulating
    , pd.token_turnover_fdv
    , pd.token_volume
FROM fees_tvl_metrics m
LEFT JOIN token_incentives_cte ti using (date)
LEFT JOIN treasury t using (date)
LEFT JOIN token_holder_data th using (date)
LEFT JOIN price_data pd using (date)
