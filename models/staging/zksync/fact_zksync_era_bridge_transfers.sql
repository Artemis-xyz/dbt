{{
    config(
        materialized="incremental",
        unique_key=["tx_hash", "event_index"],
        snowflake_warehouse="BRIDGE_MD",
    )
}}

with
    eth_deposits as (
        select
            block_timestamp,
            tx_hash,
            trace_index * 10000 as event_index,
            from_address as depositor,
            from_address as recipient,
            amount_precise_raw as amount,
            null as fee,
            '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' as token_address,
            'ethereum' as source_chain,
            'zksync' as destination_chain
        from ethereum_flipside.core.ez_native_transfers
        where
            to_address = lower('0x32400084C286CF3E17e7B677ea9583e60a000324')
            {% if is_incremental() %}

                and block_timestamp
                >= (select dateadd('day', -3, max(block_timestamp)) from {{ this }})

            {% endif %}
    ),

    eth_withdraws as (
        select
            block_timestamp,
            tx_hash,
            event_index,
            decoded_log:"to" as depositor,
            decoded_log:"to" as recipient,
            decoded_log:"amount" as amount,
            null as fee,
            '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' as token_address,
            'zksync' as source_chain,
            'ethereum' as destination_chain
        from ethereum_flipside.core.ez_decoded_event_logs
        where
            contract_address = lower('0x32400084C286CF3E17e7B677ea9583e60a000324')
            and event_name = 'EthWithdrawalFinalized'
            {% if is_incremental() %}

                and block_timestamp
                >= (select dateadd('day', -3, max(block_timestamp)) from {{ this }})

            {% endif %}
    ),

    erc20_deposits as (
        select
            block_timestamp,
            tx_hash,
            event_index,
            decoded_log:"from" as depositor,
            decoded_log:"to" as recipient,
            decoded_log:"amount" as amount,
            null as fee,
            decoded_log:"l1Token" as token_address,
            'ethereum' as source_chain,
            'zksync' as destination_chain
        from ethereum_flipside.core.ez_decoded_event_logs
        where
            contract_address = lower('0x57891966931Eb4Bb6FB81430E6cE0A03AAbDe063')
            and event_name = 'DepositInitiated'
            {% if is_incremental() %}

                and block_timestamp
                >= (select dateadd('day', -3, max(block_timestamp)) from {{ this }})

            {% endif %}
    ),

    erc20_withdraws as (
        select
            block_timestamp,
            tx_hash,
            event_index,
            decoded_log:"to" as depositor,
            decoded_log:"to" as recipient,
            decoded_log:"amount" as amount,
            null as fee,
            decoded_log:"l1Token" as token_address,
            'zksync' as source_chain,
            'ethereum' as destination_chain
        from ethereum_flipside.core.ez_decoded_event_logs
        where
            contract_address = lower('0x57891966931Eb4Bb6FB81430E6cE0A03AAbDe063')
            and event_name = 'WithdrawalFinalized'
            {% if is_incremental() %}

                and block_timestamp
                >= (select dateadd('day', -3, max(block_timestamp)) from {{ this }})

            {% endif %}
    ),

    transfers as (
        select *
        from eth_deposits
        union
        select *
        from eth_withdraws
        union
        select *
        from erc20_deposits
        union
        select *
        from erc20_withdraws
    )

select
    block_timestamp,
    tx_hash,
    event_index,
    depositor,
    recipient,
    amount,
    fee,
    token_address,
    source_chain,
    destination_chain
from
    (
        select
            *,
            row_number() over (
                partition by tx_hash, event_index order by block_timestamp asc
            ) as row_number
        from transfers
    )
where row_number = 1
