{{ config(materialized="table") }}


select
    date(timestamp) as date,
    count(distinct(id)) as txns
from
    {{ ref("fact_wormhole_transfers") }}
group by
    1
order by
    1 desc