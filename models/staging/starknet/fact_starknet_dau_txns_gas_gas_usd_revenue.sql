{{ config(materialized="table") }}

select t1.date, dau, txns, gas, gas_usd, revenue, 'starknet' as chain
from {{ ref("fact_starknet_dau") }} t1
left join {{ ref("fact_starknet_txns") }} t2 on t1.date = t2.date
left join {{ ref("fact_starknet_gas_gas_usd_revenue") }} t3 on t1.date = t3.date
where t1.date < to_date(sysdate())
