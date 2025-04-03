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
, treasury_value_cte as (
    SELECT
        date,
        token,
        SUM(native_balance) as treasury_value
    FROM {{ ref('fact_goldfinch_treasury') }}
    GROUP BY date, token
)
, treasury_native_value_cte as (
    SELECT
        date,
        token,
        SUM(native_balance) as treasury_native_value
    FROM {{ ref('fact_goldfinch_treasury') }}
    WHERE token = 'GFI'
    GROUP BY date, token
)
, net_treasury_value_cte as (
    SELECT
        date,
        token,
        SUM(native_balance) as net_treasury_value
    FROM {{ ref('fact_goldfinch_treasury') }}
    WHERE token <> 'GFI'
    GROUP BY date, token
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
    , coalesce(m.interest_fees,0) + coalesce(m.withdrawal_revenue,0) as fees
    , coalesce(m.supply_side_fees,0) as primary_supply_side_revenue
    , 0 as secondary_supply_side_revenue
    , coalesce(m.supply_side_fees,0) + secondary_supply_side_revenue as total_supply_side_revenue
    , coalesce(m.interest_revenue,0) as interest_revenue
    , coalesce(m.interest_revenue,0) + coalesce(m.withdrawal_revenue,0) as revenue
    , coalesce(ti.token_incentives,0) as token_incentives
    , 0 as operating_expenses
    , coalesce(ti.token_incentives,0) + operating_expenses as total_expenses
    , coalesce(revenue,0) - coalesce(total_expenses,0) as protocol_earnings
    , coalesce(m.net_deposits,0) as net_deposits
    , 0 as outstanding_supply
   -- , t.treasury_value as treasury_value
   -- , tn.treasury_native_value as treasury_value_native
    , coalesce(nt.net_treasury_value,0) as net_treasury_value
    , coalesce(th.token_holder_count,0) as token_holder_count

    -- Crypto Metrics
    , coalesce(m.tvl,0) as tvl
    , coalesce(m.tvl,0) - LAG(coalesce(m.tvl,0)) OVER (ORDER BY date) as tvl_net_change

    -- Cash Flow
    , coalesce(m.interest_revenue,0) + coalesce(m.withdrawal_revenue,0) as gross_protocol_revenue
    , coalesce(m.supply_side_fees,0) as service_cash_flow
    , coalesce(ti.token_incentives,0) as token_cash_flow
    , coalesce(m.interest_revenue,0) + coalesce(m.withdrawal_revenue,0) - coalesce(ti.token_incentives,0) as foundation_cash_flow
    
    -- Protocol Metrics
    , coalesce(t.treasury_value,0) as treasury
    , coalesce(tn.treasury_native_value,0) as treasury_native
    , coalesce(tn.treasury_native_value,0) - LAG(coalesce(tn.treasury_native_value,0)) 
        OVER (ORDER BY date) AS treasury_native_net_change
FROM fees_tvl_metrics m
FULL JOIN token_incentives_cte ti using (date, token)
FULL JOIN treasury_value_cte t using (date, token)
FULL JOIN treasury_native_value_cte tn using (date, token)
FULL JOIN net_treasury_value_cte nt using (date, token)
FULL JOIN token_holder_data th using (date, token)
WHERE coalesce(m.date, ti.date, t.date, tn.date, nt.date, th.date) < to_date(sysdate())