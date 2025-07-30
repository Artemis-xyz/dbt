{% macro evm_address_debits(chain) %}
    select
        block_timestamp
        , block_number
        , transaction_hash
        , from_address as address
        , contract_address
        , event_index
        , -1 as trace_index
        , -1 * amount_raw as debit_raw
        , -1 * amount_native as debit_native
    from {{ref("fact_" ~ chain ~ "_token_transfers")}}   
    where lower(from_address) not in (lower('0x0000000000000000000000000000000000000000'), lower('T9yD14Nj9j7xAB4dbGeiX9h8unkKHxuWwb'))
        and block_timestamp::date < to_date(sysdate())
        and amount_raw > 0
    {% if is_incremental() %}
        and block_timestamp >= (select dateadd('day', -3, max(block_timestamp)) from {{ this }})
    {% endif %}
    {% if chain not in ('celo', 'sonic', 'codex', 'tron', 'kaia', 'plume', 'sei', 'hyperevm', 'katana') %}
        union all
        select
            block_timestamp
            , block_number
            , transaction_hash
            , from_address as address
            , contract_address
            , -1 as event_index
            , trace_index
            , -1 * amount_raw as debit_raw
            , -1 * amount_native as debit_native
        from {{ref("fact_" ~ chain ~ "_native_token_transfers")}}   
        where lower(from_address) not in (lower('0x0000000000000000000000000000000000000000'), lower('T9yD14Nj9j7xAB4dbGeiX9h8unkKHxuWwb'))
            and block_timestamp::date < to_date(sysdate())
            and amount_raw > 0
        {% if is_incremental() %}
            and block_timestamp >= (select dateadd('day', -3, max(block_timestamp)) from {{ this }})
        {% endif %}
    {% endif %}
{% endmacro %}