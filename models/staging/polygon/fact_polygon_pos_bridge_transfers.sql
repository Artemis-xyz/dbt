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
            decoded_log:"depositor"::string as depositor,
            decoded_log:"depositReceiver"::string as recipient,
            decoded_log:"amount" as amount,
            null as fee,
            '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' as token_address,
            'ethereum' as source_chain,
            'polygon' as destination_chain
        from ethereum_flipside.core.ez_decoded_event_logs
        where
            contract_address = lower('0x8484Ef722627bf18ca5Ae6BcF031c23E6e922B30')
            and event_name = 'LockedEther'
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
            decoded_log:"exitor"::string as depositor,
            decoded_log:"exitor"::string as recipient,
            decoded_log:"amount" as amount,
            null as fee,
            '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' as token_address,
            'polygon' as source_chain,
            'ethereum' as destination_chain
        from ethereum_flipside.core.ez_decoded_event_logs
        where
            contract_address = lower('0x8484Ef722627bf18ca5Ae6BcF031c23E6e922B30')
            and event_name = 'ExitedEther'
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
            from_address::string as depositor,
            from_address::string as recipient,
            raw_amount as amount,
            null as fee,
            contract_address as token_address,
            'ethereum' as source_chain,
            'polygon' as destination_chain
        from ethereum_flipside.core.ez_token_transfers
        where
            to_address = lower('0x40ec5B33f54e0E8A33A975908C5BA1c14e5BbbDf')
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
            from_address::string as depositor,
            from_address::string as recipient,
            raw_amount as amount,
            null as fee,
            contract_address as token_address,
            'polygon' as source_chain,
            'ethereum' as destination_chain
        from ethereum_flipside.core.ez_token_transfers
        where
            from_address = lower('0x40ec5B33f54e0E8A33A975908C5BA1c14e5BbbDf')
            {% if is_incremental() %}

                and block_timestamp
                >= (select dateadd('day', -3, max(block_timestamp)) from {{ this }})

            {% endif %}
    )

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
