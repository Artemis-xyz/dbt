{{ config(materialized="table") }}
select date, daa, txns, chain
from {{ ref("fact_flow_daa_txns") }}
where date < to_date(sysdate())
