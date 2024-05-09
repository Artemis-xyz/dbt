{{ config(materialized="table") }}
select 
    block_timestamp::date as date, 
    count(DISTINCT tx_hash) as evm_txns,
    count(DISTINCT from_address) as evm_dau,
    count(DISTINCT tx_hash) / 86400 as evm_avg_tps,
    sum(tx_fee) as evm_fees_native
from sei_flipside.core_evm.fact_transactions
group by date
