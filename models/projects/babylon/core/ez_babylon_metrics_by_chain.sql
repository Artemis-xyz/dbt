{{
    config(
        materialized='table',
        snowflake_warehouse='BABYLON',
        database='BABYLON',
        schema='core',
        alias='ez_metrics_by_chain'
    )
}}

WITH tvl_data as (
    select
        date,
        tvl,
        tvl - LAG(tvl) 
        OVER (ORDER BY date) AS tvl_net_change
    from {{ ref('fact_babylon_tvl') }}
)    
, date_spine AS (
    SELECT
        date
    FROM {{ ref('dim_date_spine') }}
    where date between (select min(date) from tvl_data) and to_date(sysdate())
)

SELECT
    date_spine.date,
    'bitcoin' as chain

    -- Standardized Metrics

    -- Usage Metrics
    , tvl_data.tvl as tvl
    , tvl_data.tvl_net_change as tvl_net_change

FROM date_spine
LEFT JOIN tvl_data ON date_spine.date = tvl_data.date
WHERE date_spine.date < to_date(sysdate())
