{% macro evm_address_credits(chain) %}
    select
        block_timestamp
        , block_number
        , transaction_hash
        , to_address as address
        , contract_address
        , event_index
        , -1 as trace_index
        , amount_raw as credit_raw
        , amount_native as credit_native
    from {{ref("fact_" ~ chain ~ "_token_transfers")}}   
    where lower(to_address) not in (lower('0x0000000000000000000000000000000000000000'), lower('T9yD14Nj9j7xAB4dbGeiX9h8unkKHxuWwb'))
        and block_timestamp::date < to_date(sysdate())
    {% if is_incremental() %}
        and block_timestamp >= (select dateadd('day', -3, max(block_timestamp)) from {{ this }})
    {% endif %}
    {% if chain not in ('celo', 'sonic', 'codex', 'tron', 'kaia', 'plume') %}
        union all
        select
            block_timestamp
            , block_number
            , transaction_hash
            , to_address as address
            , contract_address
            , -1 as event_index
            , trace_index
            , amount_raw as credit_raw
            , amount_native as credit_native
        from {{ref("fact_" ~ chain ~ "_native_token_transfers")}}   
        where lower(to_address) not in (lower('0x0000000000000000000000000000000000000000'), lower('T9yD14Nj9j7xAB4dbGeiX9h8unkKHxuWwb'))
            and block_timestamp::date < to_date(sysdate())
        {% if is_incremental() %}
            and block_timestamp >= (select dateadd('day', -3, max(block_timestamp)) from {{ this }})
        {% endif %}
    {% endif %}
{% endmacro %}