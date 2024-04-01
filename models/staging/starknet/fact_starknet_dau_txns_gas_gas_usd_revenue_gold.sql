select date, dau as daa, txns, gas, gas_usd, revenue, 'starknet' as chain
from {{ ref("fact_starknet_dau_txns_gas_gas_usd_revenue") }}
