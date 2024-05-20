{{ config(materialized="table") }}

select 
    date(a.block_timestamp) as date, 
    avg(datediff(second,a.block_timestamp, b.block_timestamp)) as avg_block_time
from sei_flipside.core.fact_blocks a, sei_flipside.core.fact_blocks b
where a.block_id = b.block_id-1 
and date(a.block_timestamp) < date(sysdate())
group by 1
