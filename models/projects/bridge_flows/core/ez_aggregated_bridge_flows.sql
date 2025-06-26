{{
    config (
        materialized="table",
        snowflake_warehouse="BRIDGE_FLOWS",
        database="bridge_flows",
        schema="core",
        alias="ez_agg_flows",
    )
}}
select 
    date
    , source_chain
    , destination_chain
    , app 
    , category -- token category type
    , amount_usd
from {{ ref('agg_daily_bridge_flows_metrics') }} 
order by date desc 
