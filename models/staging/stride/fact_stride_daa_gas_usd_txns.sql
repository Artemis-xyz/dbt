{{
    config(
        materialized="view",
        snowflake_warehouse="STRIDE",
    )
}}


select 
    t1.date, 
    daa, 
    gas_usd, 
    txns,
    'stride' as chain
from {{ ref("fact_stride_txns") }} t1
left join {{ ref("fact_stride_daa") }} t2 on t1.date = t2.date
left join {{ ref("fact_stride_gas_usd") }} t3 on t1.date = t3.date
where t1.date < to_date(sysdate())
order by date desc
