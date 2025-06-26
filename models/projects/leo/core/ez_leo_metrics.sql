{{
    config(
        materialized="table",
        snowflake_warehouse="LEO",
        database="leo",
        schema="core",
        alias="ez_metrics",
    )
}}

with 
    date_spine as (
        SELECT * 
        FROM {{ ref("dim_date_spine") }}
        WHERE date < to_date(sysdate()) AND date >= (SELECT MIN(date) FROM {{ ref("fact_leo_revenue") }})
        
    )

    , leo_revenue as (
        SELECT 
            date, 
            SUM(leo_burn_amount) AS revenue_native
        FROM {{ ref("fact_leo_revenue") }}
        GROUP BY 1
    )

    , market_data as (
        {{ get_coingecko_metrics('leo-token') }}
    )

    select 
        date
        , revenue_native
        , revenue_native * price as revenue_usd
        , revenue_native AS burned_fee_allocation_native
        , revenue_native * price as burned_fee_allocation
        , revenue_native AS buybacks_native
        , revenue_native * price AS buybacks
        , revenue_native AS buyback_fee_allocation
        , SUM(revenue_native) OVER (ORDER BY date ROWS UNBOUNDED PRECEDING) AS total_burned_native
        , 1000000000 AS max_supply_native
        , 1000000000 AS total_supply_native
        , 1000000000 - SUM(revenue_native) OVER (ORDER BY date ROWS UNBOUNDED PRECEDING)  - 2469394.1 AS issued_supply_native
        , 1000000000 - SUM(revenue_native) OVER (ORDER BY date ROWS UNBOUNDED PRECEDING) - 2469394.1 AS circulating_supply_native
        , price
        , market_cap
        , fdmc
        , token_turnover_circulating
        , token_turnover_fdv
        , token_volume
    from date_spine
    left join leo_revenue using (date)
    left join market_data using (date)