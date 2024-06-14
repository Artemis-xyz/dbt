{{ config(materialized="view", snowflake_warehouse="FUSE") }}
select t1.date, t1.chain, daa, txns, gas, gas_usd
from {{ ref("fact_fuse_daa") }} t1
left join {{ ref("fact_fuse_gas_gas_usd") }} t2 on t1.date = t2.date
left join {{ ref("fact_fuse_txns") }} t3 on t1.date = t3.date
where t1.date < to_date(sysdate())
