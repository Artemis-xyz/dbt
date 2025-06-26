{{ config(snowflake_warehouse="DEBRIDGE", materialized="table") }}
select 
    date_trunc('day', src_timestamp) as date
    , sum(case when date < '2025-01-01' then coalesce(amount_sent, 0) else amount_sent end) as bridge_volume
    , sum(coalesce(percentage_fee,0) + coalesce(fix_fee,0)) as ecosystem_revenue
    , count(*) as bridge_txns
    , count(distinct depositor) as bridge_dau
from {{ ref("fact_debridge_transfers_with_price_and_metadata") }}
group by date
