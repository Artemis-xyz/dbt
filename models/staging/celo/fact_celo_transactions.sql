{{
    config(
        materialized="incremental",
        unique_key="transaction_hash",
        snowflake_warehouse="CELO_LG"
    )
}}


with 
    celo_transactions as (
        {{ clean_goldsky_transactions("celo") }}
    )
select 
     block_hash
    , block_number
    , block_timestamp
    , transaction_hash
    , transaction_index
    , from_address
    , to_address
    , fee_currency
    , coalesce(nullif(celo_transactions.gas, 0), celo_l1_transactions_31056500.gas, 0) as gas
    , coalesce(nullif(celo_transactions.gas_price, 0), celo_l1_transactions_31056500.gas_price, 0) as gas_price
    , input
    , max_fee_per_gas
    , max_priority_fee_per_gas
    , nonce
    , receipt_cumulative_gas_used
    , receipt_effective_gas_price
    , receipt_gas_used
    , receipt_status
    , transaction_type
    , value
    , id
from celo_transactions
left join {{ ref("fact_celo_l1_transactions_31056500") }} celo_l1_transactions_31056500
    using (transaction_hash)
