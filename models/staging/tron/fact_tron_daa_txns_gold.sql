{{ config(materialized="table") }}
select date, daa, txns, chain
from {{ ref("fact_tron_daa_txns") }}
