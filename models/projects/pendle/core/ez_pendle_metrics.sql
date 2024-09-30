{{
    config(
        materialized="table",
        snowflake_warehouse="PENDLE",
        database="pendle",
        schema="core",
        alias="ez_metrics",
    )
}}

with
    swap_fees as (
        SELECT
            date
            , SUM(fees_usd) as swap_fees
            , SUM(supply_side_fees) as supply_side_fees
            , SUM(revenue) as swap_revenue
        FROM
        {{ ref('fact_pendle_swap_fees') }}
        GROUP BY 1
    )
    , yield_fees as (
        SELECT
            date
            , SUM(yield_fees_usd) as yield_revenue
        FROM
            {{ ref('fact_pendle_yield_fees') }}
        GROUP BY 1
    )
    , daus_txns as (
        SELECT
            date
            , SUM(daus) as daus
            , SUM(daily_txns) as daily_txns
        FROM
            {{ ref('fact_pendle_daus_txns') }}
        GROUP BY 1
    )
    , token_incentives_cte as (
        SELECT
            date
            , SUM(token_incentives) as token_incentives
        FROM
            {{ref('fact_pendle_token_incentives_by_chain')}}
        GROUP BY 1
    )
    , tvl as (
        SELECT
            date
            , SUM(amount_usd) as tvl
            , SUM(amount_usd) as net_deposits
        FROM
            {{ref('fact_pendle_tvl_by_token_and_chain')}}
        GROUP BY 1
    )
    , treasury_value_cte as (
        select
            date,
            sum(usd_balance) as treasury_value
        from {{ref('fact_pendle_treasury')}}
        group by 1
    )
    , net_treasury_value_cte as (
        select
            date,
            sum(usd_balance) as net_treasury_value
        from {{ref('fact_pendle_treasury')}}
        where token <> 'PENDLE'
        group by 1
    )
    , treasury_value_native_cte as (
        select
            date,
            sum(native_balance) as treasury_value_native
        from {{ref('fact_pendle_treasury')}}
        where token = 'PENDLE'
        group by 1
    )
    , price_data_cte as(
        {{ get_coingecko_metrics('pendle') }}
    )
    , tokenholder_count as (
        select * from {{ref('fact_pendle_token_holders')}}
    )

SELECT
    p.date
    , d.daus as dau
    , d.daily_txns as txns
    , coalesce(yf.yield_revenue, 0) as yield_fees
    , f.swap_fees as swap_fees
    , yield_fees + swap_fees as fees
    , f.supply_side_fees as primary_supply_side_revenue
    , 0 as secondary_supply_side_revenue
    , primary_supply_side_revenue + secondary_supply_side_revenue as total_supply_side_revenue
    , f.swap_revenue as swap_revenue_vependle
    , coalesce(yf.yield_revenue, 0) as yield_revenue_vependle
    , swap_revenue_vependle + yield_revenue_vependle as total_revenue_vependle
    , 0 as protocol_revenue
    , coalesce(token_incentives, 0) as token_incentives
    , 0 as operating_expenses
    , token_incentives + operating_expenses as total_expenses
    , protocol_revenue - total_expenses as protocol_earnings
    , tv.treasury_value
    , tn.treasury_value_native
    , nt.net_treasury_value
    , t.tvl
    , t.net_deposits
    , 0 as outstanding_supply
    , p.fdmc
    , p.market_cap
    , p.token_turnover_fdv
    , p.token_turnover_circulating
    , p.token_volume
    , tc.token_holder_count
FROM price_data_cte p
LEFT JOIN swap_fees f using(date)
LEFT JOIN yield_fees yf using(date)
LEFT JOIN daus_txns d using(date)
LEFT JOIN token_incentives_cte using(date)
LEFT JOIN tvl t USING (date)
LEFT JOIN treasury_value_cte tv USING (date)
LEFT JOIN net_treasury_value_cte nt USING (date)
LEFT JOIN treasury_value_native_cte tn USING (date) 
LEFT JOIN tokenholder_count tc using(date)