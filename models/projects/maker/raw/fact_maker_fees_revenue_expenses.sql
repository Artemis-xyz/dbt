{{
    config(
        materialized="table",
        snowflake_warehouse="MAKER",
        database="maker",
        schema="raw",
        alias="fact_maker_fees_revenue_expenses"
    )
}}


select
    date(period) as date,
    sum(case
            when
                account_id like '311%' AND account_id NOT IN ('31172', '31173', '31180') and sum_value >= 0 -- Stability fees
            then sum_value
        end
    ) as stability_fees,
    sum(case
            when account_id like '313%' and sum_value >= 0 -- Trading fees
            then sum_value
        end
    ) as trading_fees,
    sum(case
            when
                account_id like '311%' AND account_id NOT IN ('31172', '31173', '31180') -- Stability fees
                or account_id like '313%' -- Trading fees
            then sum_value
        end
    ) as fees,
    sum(
        case
            when account_id like '311%'  -- Gross Interest Revenues
            then sum_value
        end
    ) as primary_revenue,
    sum(
        case
            when account_id like '312%'  -- Liquidation Revenues
                or account_id like '313%'  -- Trading Revenues
            then sum_value
        end
    ) as other_revenue,
    sum(
        case
            when account_id like '312%'  -- Liquidation Revenues
            then sum_value
        end
    ) as liquidation_revenue,
    sum(
        case
            when account_id like '313%'  -- Trading Revenues
            then sum_value
        end
    ) as trading_revenue,
    sum(
        case
            when account_id like '312%'  -- Liquidation Revenues
                or account_id like '313%'  -- Trading Revenues
                or account_id like '311%'  -- Gross Interest Revenues
            then sum_value
        end
    ) as protocol_revenue,
    sum(
        case
            when account_id = '31410' and sum_value < 6e7 -- MKR Mints (filter out large 60m transfer)
            then sum_value
        end
    ) as token_incentives,
    sum(
        case
            when account_id like '316%'  -- Direct Expenses
            then -sum_value
        end
    ) as direct_expenses,
    sum(
        case
            when (account_id like '317%'  -- Indirect Expenses
                or account_id like '321%'  -- MKR Token Expenses
                or account_id like '331%'  -- DS Pause Proxy
            ) and sum_value < 5e7
            then -sum_value
        end
    ) as operating_expenses,
    sum(
        case
            when (account_id like '316%'  -- Direct Expenses
                or account_id like '317%'  -- Indirect Expenses
                or account_id like '321%'  -- MKR Token Expenses
                or account_id like '331%'  -- DS Pause Proxy
                or account_id = '31410') 
                and sum_value < 5e7
            then -sum_value
        end
    ) as total_expenses
from  {{ ref('fact_accounting_agg') }}
group by 1