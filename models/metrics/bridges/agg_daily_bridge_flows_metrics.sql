{{ config(materialized="table") }}
select
    date, source_chain, destination_chain, app, coalesce(category, 'Not Categorized') as category, amount_usd, fee_usd, unique_id
from {{ ref("agg_daily_bridge_flows_metrics_silver") }}
