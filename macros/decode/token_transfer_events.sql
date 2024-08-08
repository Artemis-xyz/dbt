{% macro token_transfer_events(chain) %}
    select
        block_timestamp,
        block_number,
        transaction_hash,
        event_index,
        origin_from_address,
        origin_to_address,
        contract_address,
        decoded_log:"from"::string as from_address,
        decoded_log:"to"::string as to_address,
        try_to_number(decoded_log:"value"::string) as amount,
        tx_status
    from {{ ref("fact_" ~ chain ~ "_decoded_events") }}
    where topic_zero = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
        and data != '0x'
        and amount is not null
        and tx_status = 1
    {% if is_incremental() %}
        and block_timestamp >= (select dateadd('day', -3, max(block_timestamp)) from {{ this }})
    {% endif %}
    {% if chain in ('celo') %}
        union all
        select
            t1.block_timestamp,
            t1.block_number,
            t1.transaction_hash,
            null as event_index,
            t1.from_address as origin_from_address,
            t1.to_address as origin_to_address,
            t1.fee_currency as contract_address,
            t1.from_address,
            t2.miner as to_address,
            t1.gas * t1.gas_price as amount,
            1 as tx_status -- Fees are paid whether or not the transaction is successful
        from {{ref("fact_celo_transactions")}} t1
        left join {{ref("fact_celo_blocks")}} t2
            using (block_number)
        where fee_currency is not null 
         {% if is_incremental() %}
            and block_timestamp >= (select dateadd('day', -3, max(block_timestamp)) from {{ this }})
        {% endif %}

    {% endif %}
{% endmacro %}
