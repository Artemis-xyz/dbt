{{ config(materialized="table") }}
select date, daa, txns, gas, gas_usd, chain, source
from {{ ref("fact_zksync_daa_txns_gas_gas_usd") }}
