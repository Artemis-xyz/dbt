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
            event_index,
            decoded_log:"sender"::string as depositor,
            decoded_log:"sender"::string as recipient,
            decoded_log:"amount" as amount,
            null as fee,
            '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' as token_address,
            'ethereum' as source_chain,
            'starknet' as destination_chain
        from ethereum_flipside.core.ez_decoded_event_logs
        where
            contract_address = lower('0xae0Ee0A63A2cE6BaeEFFE56e7714FB4EFE48D419')
            and event_name = 'LogDeposit'
            {% if is_incremental() %}

                and block_timestamp
                > (select dateadd('day', -3, max(block_timestamp)) from {{ this }})

            {% endif %}
    ),

    eth_withdraws as (
        select
            block_timestamp,
            tx_hash,
            event_index,
            decoded_log:"recipient"::string as depositor,
            decoded_log:"recipient"::string as recipient,
            decoded_log:"amount" as amount,
            null as fee,
            '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' as token_address,
            'starknet' as source_chain,
            'ethereum' as destination_chain
        from ethereum_flipside.core.ez_decoded_event_logs
        where
            contract_address = lower('0xae0Ee0A63A2cE6BaeEFFE56e7714FB4EFE48D419')
            and event_name = 'LogWithdrawal'
            {% if is_incremental() %}

                and block_timestamp
                > (select dateadd('day', -3, max(block_timestamp)) from {{ this }})

            {% endif %}
    ),

    erc20_bridges as (
        select distinct decoded_log:"fromAddress"::string as bridge_address
        from ethereum_flipside.core.ez_decoded_event_logs
        where
            contract_address = lower('0xc662c410C0ECf747543f5bA90660f6ABeBD9C8c4')
            and event_name = 'LogMessageToL2'
    ),

    erc20_deposits as (
        select
            block_timestamp,
            tx_hash,
            event_index,
            from_address::string as depositor,
            from_address::string as recipient,
            raw_amount as amount,
            null as fee,
            contract_address as token_address,
            'ethereum' as source_chain,
            'starknet' as destination_chain
        from ethereum_flipside.core.ez_token_transfers
        where
            to_address in (select * from erc20_bridges)
            {% if is_incremental() %}

                and block_timestamp
                > (select dateadd('day', -3, max(block_timestamp)) from {{ this }})

            {% endif %}
    ),

    erc20_withdraws as (
        select
            block_timestamp,
            tx_hash,
            event_index,
            to_address::string as depositor,
            to_address::string as recipient,
            raw_amount as amount,
            null as fee,
            contract_address as token_address,
            'starknet' as source_chain,
            'ethereum' as destination_chain
        from ethereum_flipside.core.ez_token_transfers
        where
            from_address in (select * from erc20_bridges)
            {% if is_incremental() %}

                and block_timestamp
                > (select dateadd('day', -3, max(block_timestamp)) from {{ this }})

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
