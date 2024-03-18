{{ config(materialized="table") }}
select date, daa, txns, chain, gas, gas_usd
from {{ ref("fact_fuse_daa_txns_gas_gas_usd") }}
