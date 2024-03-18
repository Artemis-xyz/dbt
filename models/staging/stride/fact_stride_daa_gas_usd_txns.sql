select t1.date, daa, gas_usd, txns, t1.chain
from {{ ref("fact_stride_daa") }} t1
join {{ ref("fact_stride_txns") }} t2 on t1.date = t2.date
join {{ ref("fact_stride_gas_usd") }} t3 on t1.date = t3.date
where t1.date < to_date(sysdate())
order by date
