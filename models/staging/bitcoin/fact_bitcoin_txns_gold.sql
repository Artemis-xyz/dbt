{{ config(materialized="table") }}
select date, txns, source, chain
from {{ ref("fact_bitcoin_txns") }}
