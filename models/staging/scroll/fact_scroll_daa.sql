{{ config(materialized="table", snowflake_warehouse="SCROLL") }}
select
    to_timestamp(block_timestamp)::date as date,
    coalesce(count(distinct from_address), 0) as daa,
    'scroll' as chain
from {{ ref("fact_scroll_transactions") }}
where receipt_status = 1
group by 1
order by 1 asc
