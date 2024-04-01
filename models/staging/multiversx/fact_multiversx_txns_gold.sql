{{ config(materialized="table") }}
select date, txns, chain
from {{ ref("fact_multiversx_txns") }}
where date < to_date(sysdate())
