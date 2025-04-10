{{
    config(
        materialized="table",
        snowflake_warehouse="MAKER",
        database="maker",
        schema="raw",
        alias="fact_maker_fees_revenue_expenses_by_token"
    )
}}


select
    date(period) as date,
    token,
    sum(case
            when
                account_id like '311%' AND account_id NOT IN ('31172', '31173', '31180')-- Stability fees
            then sum_value_token
        end
    ) as stability_fees,
    sum(case
            when account_id like '313%' -- Trading fees
            then sum_value_token
        end
    ) as trading_fees,
    sum(case
            when
                account_id like '311%' AND account_id NOT IN ('31172', '31173', '31180') -- Stability fees
                or account_id like '313%' -- Trading fees
            then sum_value_token
        end
    ) as fees,
    sum(
        case
            when account_id like '311%'  -- Gross Interest Revenues
            then sum_value_token
        end
    ) as primary_revenue,
    sum(
        case
            when account_id like '312%'  -- Liquidation Revenues
            then sum_value_token
        end
    ) as liquidation_revenue,
    sum(
        case
            when account_id like '312%'  -- Liquidation Revenues
                or account_id like '313%'  -- Trading Revenues
            then sum_value_token
        end
    ) as other_revenue,
    sum(
        case
            when account_id like '312%'  -- Liquidation Revenues
                or account_id like '313%'  -- Trading Revenues
                or account_id like '311%'  -- Gross Interest Revenues
            then sum_value_token
        end
    ) as protocol_revenue,
    sum(
        case
            when account_id = '31410' -- MKR Mints
        then sum_value_token
        end
    ) as token_incentives,
    sum(
        case
            when account_id like '316%'  -- Direct Expenses
            then -sum_value_token
        end
    ) as direct_expenses,
    sum(
        case
            when account_id like '317%'  -- Indirect Expenses
                or account_id like '321%'  -- MKR Token Expenses
                or account_id like '331%'  -- DS Pause Proxy
            then -sum_value_token
        end
    ) as operating_expenses,
    sum(
        case
            when account_id like '316%'  -- Direct Expenses
                or account_id like '317%'  -- Indirect Expenses
                or account_id like '321%'  -- MKR Token Expenses
                or account_id like '331%'  -- DS Pause Proxy
                or account_id = '31410'
            then -sum_value_token
        end
    ) as total_expenses
from  {{ ref('fact_accounting_agg') }}
group by 1, 2