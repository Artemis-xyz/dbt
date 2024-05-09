{{ config(materialized="table") }}

select 
    date(a.block_timestamp) as date, 
    avg(datediff(second,a.block_timestamp, b.block_timestamp)) as evm_avg_bps
from sei_flipside.core_evm.fact_blocks a, sei.core_evm.fact_blocks b
where a.BLOCK_NUMBER = b.BLOCK_NUMBER-1 
group by 1
