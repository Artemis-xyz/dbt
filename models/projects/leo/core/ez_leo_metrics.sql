{{
    config(
        materialized="incremental",
        snowflake_warehouse="LEO",
        database="leo",
        schema="core",
        alias="ez_metrics",
        incremental_strategy="merge",
        unique_key="date",
        on_schema_change="append_new_columns",
        merge_update_columns=var("backfill_columns", []),
        merge_exclude_columns=["created_on"] if not var("backfill_columns", []) else none,
        full_refresh=false,
        tags=["ez_metrics"],
    )
}}

{% set backfill_date = var("backfill_date", None) %}

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
        , coalesce(revenue_native, 0) AS revenue_native
        , coalesce(revenue_native, 0) * price as revenue
        , coalesce(revenue_native, 0) AS burns_native
        , coalesce(revenue_native, 0) AS buybacks_native
        , coalesce(revenue_native, 0) * price AS buybacks
        -- Supply metrics
        -- The 1B is the total LEO tokens sold during their Initial Exchange Offering (IEO) in a Private Sale. 
        , 1000000000 AS max_supply_native
        , 1000000000 AS total_supply_native
        -- There is a 2,469,394.1 discrepancy between the 1B - Burns calculation, but this can be attributed to burns events that happened on the exchange directly through
        -- recovered funds from Crypto Capital and the Bitfinex Hack. (https://www.bitfinex.com/wp-2019-05.pdf)
        , 1000000000 - SUM(coalesce(revenue_native, 0)) OVER (ORDER BY date ROWS UNBOUNDED PRECEDING)  - 2469394.1 AS issued_supply_native
        , 1000000000 - SUM(coalesce(revenue_native, 0)) OVER (ORDER BY date ROWS UNBOUNDED PRECEDING) - 2469394.1 AS circulating_supply_native
        -- Token Turnover Metrics
        , token_turnover_circulating
        , token_turnover_fdv
        -- timestamp columns
        , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
        , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on
    from date_spine
    left join leo_revenue using (date)
    left join market_data using (date)
    where true
    {{ ez_metrics_incremental('date', backfill_date) }}
    and date < to_date(sysdate())