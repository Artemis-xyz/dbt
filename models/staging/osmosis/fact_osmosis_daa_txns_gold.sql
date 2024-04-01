{{ config(materialized="table") }}
select date, daa, txns, chain
from {{ ref("fact_osmosis_daa_txns") }}
