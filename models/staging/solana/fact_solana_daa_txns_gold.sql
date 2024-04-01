{{ config(materialized="table") }}

select daa, date, txns, chain
from {{ ref("fact_solana_daa_txns") }}
where date < to_date(sysdate())
