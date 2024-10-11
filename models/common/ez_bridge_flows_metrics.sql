{{ 
    config(
        snowflake_warehouse="COMMON",
        database="common",
        schema="core",
        materialized='table'
    )
}}

select 
    date
    , source_chain
    , destination_chain
    , app
    , sum(amount_usd) as transfer_volume
from {{ ref('agg_daily_bridge_flows_metrics_silver') }} 
where amount_usd > 0
group by date, source_chain, destination_chain, app
order by date desc