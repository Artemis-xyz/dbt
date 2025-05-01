{{
    config(
        materialized='table',
        snowflake_warehouse='BABYLON',
        database='BABYLON',
        schema='core',
        alias='ez_metrics_by_chain'
    )
}}

with tvl_data as (
    select
        date,
        tvl,
        tvl - LAG(tvl) 
        OVER (order by date) as tvl_net_change
    from {{ ref('fact_babylon_tvl') }}
)    
, date_spine as (
    select
        date
    from {{ ref('dim_date_spine') }}
    where date between (select min(date) from tvl_data) and to_date(sysdate())
)

select
    date_spine.date,
    'bitcoin' as chain

    -- Standardized Metrics

    -- Usage Metrics
    , tvl_data.tvl as tvl
    , tvl_data.tvl_net_change as tvl_net_change

from date_spine
left join tvl_data on date_spine.date = tvl_data.date
where date_spine.date <= to_date(sysdate())
