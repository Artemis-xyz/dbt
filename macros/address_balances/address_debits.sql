{% macro address_debits(chain, wrapped_native_token_address) %}

    select
        from_address as address,
        'native_token' as contract_address,
        block_timestamp,
        tx_fee * -1 as debit,
        null as debit_usd,
        tx_hash,
        -1 as trace_index,
        -1 as event_index
    from {{ chain }}_flipside.core.fact_transactions as t
    where
        to_date(block_timestamp) < to_date(sysdate())
        and (tx_succeeded or tx_fee > 0)
        and from_address <> lower('0x0000000000000000000000000000000000000000')
        {% if is_incremental() %}
            and block_timestamp
            >= (select dateadd('day', -3, max(block_timestamp)) from {{ this }})
        {% endif %}

    union all

    select
        from_address as address,
        contract_address,
        block_timestamp,
        cast(raw_amount * -1 as float) as debit,
        amount_usd * -1 as debit_usd,
        tx_hash,
        -1 as trace_index,
        event_index
    from {{ chain }}_flipside.core.ez_token_transfers
    where
        to_date(block_timestamp) < to_date(sysdate())
        and from_address <> lower('0x0000000000000000000000000000000000000000')
        {% if is_incremental() %}
            and block_timestamp
            >= (select dateadd('day', -3, max(block_timestamp)) from {{ this }})
        {% endif %}

    union all

    select
        from_address as address,
        'native_token' as contract_address,
        block_timestamp,
        amount * -1 as debit,
        amount_usd * -1 as debit_usd,
        tx_hash,
        trace_index,
        -1 as event_index
    from {{ chain }}_flipside.core.ez_native_transfers
    where
        to_date(block_timestamp) < to_date(sysdate())
        and from_address <> lower('0x0000000000000000000000000000000000000000')
        and from_address <> to_address
        {% if is_incremental() %}
            and block_timestamp
            >= (select dateadd('day', -3, max(block_timestamp)) from {{ this }})
        {% endif %}

    {% if wrapped_native_token_address is defined %}
        union all
        select
            decoded_log:"src"::string as address,
            contract_address,
            block_timestamp,
            cast(decoded_log:"wad" as float) * -1 as debit,
            null as debit_usd,
            tx_hash,
            -1 as trace_index,
            event_index
        from {{ chain }}_flipside.core.ez_decoded_event_logs
        where
            event_name = 'Withdrawal'
            and contract_address = lower('{{ wrapped_native_token_address }}')
            and to_date(block_timestamp) < to_date(sysdate())
            and address <> lower('0x0000000000000000000000000000000000000000')
            {% if is_incremental() %}
                and block_timestamp
                >= (select dateadd('day', -3, max(block_timestamp)) from {{ this }})
            {% endif %}
    {% endif %}
{% endmacro %}
