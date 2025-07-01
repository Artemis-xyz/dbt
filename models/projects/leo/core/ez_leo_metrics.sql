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
        -- Standardized Metrics
        -- Market Data Metrics
        , price
        , market_cap
        , fdmc
        , token_volume

        -- Fee Allocation Metrics
        , revenue_native
        , revenue_native * price as revenue
        , revenue_native AS burns_native
        , revenue_native AS buybacks_native
        , revenue_native * price AS buybacks
        -- Supply metrics
        -- The 1B is the total LEO tokens sold during their Initial Exchange Offering (IEO) in a Private Sale. 
        , 1000000000 AS max_supply_native
        , 1000000000 AS total_supply_native
        -- There is a 2,469,394.1 discrepancy between the 1B - Burns calculation, but this can be attributed to burns events that happened on the exchange directly through
        -- recovered funds from Crypto Capital and the Bitfinex Hack. (https://www.bitfinex.com/wp-2019-05.pdf)
        , 1000000000 - SUM(revenue_native) OVER (ORDER BY date ROWS UNBOUNDED PRECEDING)  - 2469394.1 AS issued_supply_native
        , 1000000000 - SUM(revenue_native) OVER (ORDER BY date ROWS UNBOUNDED PRECEDING) - 2469394.1 AS circulating_supply_native

        -- Token Turnover Metrics
        , token_turnover_circulating
        , token_turnover_fdv
    from date_spine
    left join leo_revenue using (date)
    left join market_data using (date)