
{{
    config(
        materialized="incremental",
        unique_key=["tx_hash", "event_index", "trace_index"],
        snowflake_warehouse="BALANCES_MD",
    )
}}

 select
        from_address as address,
        'eip155:1329:native' as contract_address,
        block_timestamp,
        tx_fee * -1 as debit,
        tx_hash,
        -1 as trace_index,
        -1 as event_index
    from sei_flipside.core_evm.fact_transactions as t
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
        contract_address,
        block_timestamp,
        cast(raw_amount * -1 as float) as debit,
        tx_hash,
        -1 as trace_index,
        event_index
    from sei_flipside.core_evm.ez_token_transfers
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
        'eip155:1329:native' as contract_address,
        block_timestamp,
        amount * -1 as debit,
        tx_hash,
        trace_index,
        -1 as event_index
    from sei_flipside.core_evm.ez_native_transfers
    where
        to_date(block_timestamp) < to_date(sysdate())
        and from_address <> lower('0x0000000000000000000000000000000000000000')
        and from_address <> to_address
        {% if is_incremental() %}
            and block_timestamp
            >= (select dateadd('day', -3, max(block_timestamp)) from {{ this }})
        {% endif %}
