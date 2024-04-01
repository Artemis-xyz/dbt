{{ config(materialized="table") }}
select date, txns, chain, source
from {{ ref("fact_fantom_txns") }}
