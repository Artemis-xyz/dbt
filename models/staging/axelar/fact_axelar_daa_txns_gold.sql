{{ config(materialized="table") }}
select date, daa, txns, chain
from {{ ref("fact_axelar_daa_txns") }}
