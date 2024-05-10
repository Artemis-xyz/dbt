{{ config(materialized="table") }}

select 
    date(a.block_timestamp) as date, 
    avg(datediff(second,a.block_timestamp, b.block_timestamp)) as avg_bps
from sei_flipside.core.fact_blocks a, sei.core_evm.fact_blocks b
where a.BLOCK_NUMBER = b.BLOCK_NUMBER-1 
and date(a.block_timestamp) < date(sysdate())
group by 1
