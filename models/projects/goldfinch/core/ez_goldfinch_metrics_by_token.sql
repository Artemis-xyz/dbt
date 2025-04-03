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
    coalesce(m.date, ti.date, t.date, tn.date, nt.date, th.date) as date
    , coalesce(m.token, ti.token, t.token, tn.token, nt.token, th.token) as token
    , coalesce(m.interest_fees,0) as interest_fees
    , coalesce(m.withdrawal_revenue,0) as withdrawal_revenue
    , coalesce(m.interest_fees,0) as interest_fees
    , coalesce(m.withdrawal_revenue,0) as withdrawal_revenue
    , coalesce(m.interest_fees,0) + coalesce(m.withdrawal_revenue,0) as fees
    , coalesce(m.supply_side_fees,0) as primary_supply_side_revenue
    , coalesce(m.supply_side_fees,0) as primary_supply_side_revenue
    , 0 as secondary_supply_side_revenue
    , coalesce(m.supply_side_fees,0) + secondary_supply_side_revenue as total_supply_side_revenue
    , coalesce(m.interest_revenue,0) as interest_revenue
    , coalesce(m.supply_side_fees,0) + secondary_supply_side_revenue as total_supply_side_revenue
    , coalesce(m.interest_revenue,0) as interest_revenue
    , coalesce(m.interest_revenue,0) + coalesce(m.withdrawal_revenue,0) as revenue
    , coalesce(ti.token_incentives,0) as token_incentives
    , 0 as operating_expenses
    , coalesce(ti.token_incentives,0) + operating_expenses as total_expenses
    , coalesce(revenue,0) - coalesce(total_expenses,0) as protocol_earnings
    , coalesce(m.net_deposits,0) as net_deposits
    , coalesce(ti.token_incentives,0) + operating_expenses as total_expenses
    , coalesce(revenue,0) - coalesce(total_expenses,0) as protocol_earnings
    , coalesce(m.net_deposits,0) as net_deposits
    , 0 as outstanding_supply
    , coalesce(nt.net_treasury,0) as net_treasury_value
    , coalesce(th.token_holder_count,0) as token_holder_count

    -- Standardized Metrics

    -- Lending Metrics
    , coalesce(outstanding_supply,0) as lending_loans
    , coalesce(m.net_deposits,0) as lending_deposits
    , coalesce(m.tvl,0) + coalesce(outstanding_supply,0) as lending_loan_capacity
    , coalesce(m.interest_fees,0) as lending_interest_fees

    -- Crypto Metrics
    , coalesce(m.tvl,0) as tvl
    , coalesce(m.tvl,0) - LAG(coalesce(m.tvl,0)) OVER (ORDER BY date) as tvl_net_change

    -- Cash Flow
    , coalesce(m.interest_fees,0) + coalesce(m.withdrawal_revenue,0) as gross_protocol_revenue
    , coalesce(m.supply_side_fees,0) as service_cash_flow
    , coalesce(ti.token_incentives,0) as fee_sharing_token_cash_flow
    , 0.1*coalesce(m.interest_fees,0) + coalesce(m.withdrawal_revenue,0) as treasury_cash_flow
    
    -- Protocol Metrics
    , coalesce(t.treasury, 0) as treasury
    , coalesce(t.treasury_native, 0) as treasury_native
    , coalesce(nt.net_treasury, 0) as net_treasury
    , coalesce(nt.net_treasury_native, 0) as net_treasury_native
    , coalesce(tn.own_token_treasury, 0) as own_token_treasury
    , coalesce(tn.own_token_treasury_native, 0) as own_token_treasury_native
FROM fees_tvl_metrics m
FULL JOIN token_incentives_cte ti using (date, token)
FULL JOIN treasury_by_token t using (date, token)
FULL JOIN treasury_native tn using (date, token)
FULL JOIN net_treasury nt using (date, token)
FULL JOIN token_holder_data th using (date, token)
WHERE coalesce(m.date, ti.date, t.date, tn.date, nt.date, th.date) < to_date(sysdate())