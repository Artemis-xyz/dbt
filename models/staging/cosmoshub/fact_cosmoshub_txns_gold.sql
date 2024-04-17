{{ config(materialized="table") }}
select date, txns, chain
from {{ ref("fact_cosmoshub_txns") }}
