{{ config(materialized="table") }}


select
    date(src_timestamp) as date,
    count(distinct(id)) as txns
from
    {{ ref("fact_wormhole_operations_with_price") }}
group by
    1
order by
    1 desc