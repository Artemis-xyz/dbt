{{ config(materialized="table") }}
select date, daa, txns, chain, gas_usd
from {{ ref("fact_polygon_zk_daa_txns_gas_usd") }}
