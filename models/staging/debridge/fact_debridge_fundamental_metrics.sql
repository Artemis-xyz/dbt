{{ config(snowflake_warehouse="DEBRIDGE", materialized="table") }}
select 
    date_trunc('day', src_timestamp) as date
    , sum(amount_sent) as bridge_volume
    , sum(coalesce(percentage_fee,0) + coalesce(fix_fee,0)) as ecosystem_revenue
    , count(*) as bridge_txns
from {{ ref("fact_debridge_transfers_with_prices") }}
group by date
