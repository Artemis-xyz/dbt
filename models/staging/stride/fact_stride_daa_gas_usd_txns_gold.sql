{{ config(materialized="table") }}

select date, daa, gas_usd, txns, chain
from {{ ref("fact_stride_daa_gas_usd_txns") }}
