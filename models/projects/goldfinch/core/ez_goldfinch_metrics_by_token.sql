{{
    config(
        materialized="table"
        , snowflake_warehouse="GOLDFINCH"
        , database="goldfinch"
        , schema="core"
        , alias="ez_metrics_by_token"
    )
}}  

with fees_tvl_metrics as(
    SELECT
        date,
        'USDC' as token,
        SUM(interest_fees) as interest_fees,
        SUM(supply_side_fees) as supply_side_fees, 
        SUM(interest_revenue) as interest_revenue,
        SUM(withdrawal_revenue) as withdrawal_revenue,
        AVG(net_deposits) as net_deposits,
        AVG(tvl) as tvl
    FROM {{ ref('fact_goldfinch_metrics') }}
    GROUP BY date
)
, token_incentives_cte as (
    SELECT
        date,
        token,
        SUM(amount_native) as token_incentives
    FROM {{ ref('fact_goldfinch_token_incentives') }}
    GROUP BY date, token
)
, treasury_by_token as (
    select
        date,
        token,
        sum(usd_balance) as treasury,
        sum(native_balance) as treasury_native
    from {{ ref('fact_goldfinch_treasury') }}
    group by 1, 2
)
, net_treasury as (
    select
        date,
        token,
        sum(usd_balance) as net_treasury,
        sum(native_balance) as net_treasury_native
    from {{ ref('fact_goldfinch_treasury') }}
    where token != 'GFI'
    group by 1, 2
)
, treasury_native as (
    select
        date,
        token,
        sum(usd_balance) as own_token_treasury,
        sum(native_balance) as own_token_treasury_native
    from {{ ref('fact_goldfinch_treasury') }}
    where token = 'GFI'
    group by 1, 2
)  
, token_holder_data as (
    select
        date
        , token_holder_count
        , 'GFI' as token
    from {{ ref("fact_goldfinch_tokenholders")}}
)

SELECT
    coalesce(m.date, ti.date, treasury_by_token.date, treasury_native.date, net_treasury.date, th.date) as date
    , coalesce(m.token, ti.token, treasury_by_token.token, treasury_native.token, net_treasury.token, th.token) as token
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
    , token_incentives + operating_expenses as total_expenses
    , revenue - total_expenses as earnings
    , m.net_deposits as net_deposits
    , 0 as outstanding_supply
    , treasury_by_token.treasury as treasury_value
    , treasury_by_token.treasury_native as treasury_value_native
    , net_treasury.net_treasury as net_treasury_value
    , token_holder_count

    -- Standardized Metrics

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
    , coalesce(treasury_by_token.treasury, 0) as treasury
    , coalesce(treasury_by_token.treasury_native, 0) as treasury_native
    , coalesce(net_treasury.net_treasury, 0) as net_treasury
    , coalesce(net_treasury.net_treasury_native, 0) as net_treasury_native
    , coalesce(treasury_native.own_token_treasury, 0) as own_token_treasury
    , coalesce(treasury_native.own_token_treasury_native, 0) as own_token_treasury_native
FROM fees_tvl_metrics m
FULL JOIN token_incentives_cte ti using (date, token)
FULL JOIN treasury_by_token using (date, token)
FULL JOIN net_treasury using (date, token)
FULL JOIN treasury_native using (date, token)
FULL JOIN token_holder_data th using (date, token)
WHERE coalesce(m.date, ti.date, treasury_by_token.date, treasury_native.date, net_treasury.date, th.date) < to_date(sysdate())