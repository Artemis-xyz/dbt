{{
    config(
        materialized="incremental",
        unique_key="date",
    )
}}
select 
    date(a.block_timestamp) as date, 
    avg(datediff(second,a.block_timestamp, b.block_timestamp)) as evm_avg_block_time
from sei_flipside.core_evm.fact_blocks a, sei_flipside.core_evm.fact_blocks b
where a.block_number = b.block_number-1 
and date(a.block_timestamp) < date(sysdate())
and date(b.block_timestamp) < date(sysdate())
{% if is_incremental() %}
    and a.block_timestamp >= (select dateadd('day', -3, max(date)) from {{ this }})
    and b.block_timestamp >= (select dateadd('day', -3, max(date)) from {{ this }})
{% endif %}
group by 1
