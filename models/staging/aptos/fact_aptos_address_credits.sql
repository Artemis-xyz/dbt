{{ config(snowflake_warehouse="BALANCES_LG", materialized="incremental", unique_key=["transaction_hash", "event_index"]) }}

SELECT 
    block_timestamp,
    block_number,
    tx_hash as transaction_hash,
    account_address as address,
    token_address as contract_address,
    event_index AS event_index,
    -1 as trace_index,
    amount AS credit_raw,
    amount / pow(10, decimals) AS credit_native
FROM aptos_flipside.core.fact_transfers
left join aptos_flipside.core.dim_tokens using (token_address)
WHERE transfer_event = 'DepositEvent'
    and block_timestamp::date < to_date(sysdate())
    {% if is_incremental() %}
        and block_timestamp > (select dateadd('day', -3, max(block_timestamp)) from {{ this }})
    {% endif %}