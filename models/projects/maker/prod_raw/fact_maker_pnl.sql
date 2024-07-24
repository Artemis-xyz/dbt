{{
    config(
        materialized="table",
        snowflake_warehouse="MAKER",
        database="maker",
        schema="raw",
        alias="fact_maker_pnl"
    )
}}

with -- Maker - Accounting Aggregated v2
    pnl as (
        select
            period as date,
            sum(
                case
                    when account_id like '312%'  -- Liquidation Revenues
                    then sum_value
                end
            ) as liquidation_income,
            sum(
                case
                    when account_id like '313%'  -- Trading Revenues
                      or account_id like '3119%' -- Stablecoins
                    then sum_value
                end
            ) as trading_income,
            sum(
                case
                    when account_id like '311%'  -- Gross Interest Revenues
                    then sum_value
                end
            ) as lending_income,
            sum(
                case
                    when account_id like '316%'  -- Direct Expenses
                    then sum_value
                end
            ) as direct_expenses,
            sum(
                case
                    when account_id like '317%'  -- Indirect Expenses
                      or account_id like '321%'  -- MKR Token Expenses
                      or account_id like '331%'  -- DS Pause Proxy
                    then sum_value
                end
            ) as operating_expenses
        from  {{ ref('fact_accounting_agg') }}
        group by 1
    )
    
select
    date_trunc('day', date) as date,
    SUM(iff(liquidation_income < 1e-4, 0, liquidation_income)) as "Liquidation Income",
    SUM(iff(trading_income < 1e-4, 0, trading_income)) as "Trading Fees",
    SUM(iff(lending_income < 1e-4, 0, lending_income)) as "Interest Income",
    SUM(direct_expenses) as "Direct Expenses",
    SUM(operating_expenses) as "Operating Expenses"
from pnl
group by 1
order by 1 desc