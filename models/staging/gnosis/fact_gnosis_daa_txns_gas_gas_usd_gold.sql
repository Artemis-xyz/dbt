{{ config(materialized="table") }}
select date, daa, txns, gas, gas_usd, chain
from {{ ref("fact_gnosis_daa_txns_gas_gas_usd") }}
where date < to_date(sysdate())
