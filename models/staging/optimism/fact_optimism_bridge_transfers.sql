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
            coalesce(decoded_log:"from", decoded_log:"_from") as depositor,
            coalesce(decoded_log:"to", decoded_log:"_to") as recipient,
            coalesce(decoded_log:"amount", decoded_log:"_amount") as amount,
            null as fee,
            '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' as token_address,
            'ethereum' as source_chain,
            'optimism' as destination_chain
        from ethereum_flipside.core.ez_decoded_event_logs
        where
            contract_address = lower('0x99C9fc46f92E8a1c0deC1b1747d010903E884bE1')
            and event_name = 'ETHDepositInitiated'
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
            coalesce(decoded_log:"from", decoded_log:"_from") as depositor,
            coalesce(decoded_log:"to", decoded_log:"_to") as recipient,
            coalesce(decoded_log:"amount", decoded_log:"_amount") as amount,
            null as fee,
            '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' as token_address,
            'optimism' as source_chain,
            'ethereum' as destination_chain
        from ethereum_flipside.core.ez_decoded_event_logs
        where
            contract_address = lower('0x99C9fc46f92E8a1c0deC1b1747d010903E884bE1')
            and event_name = 'ETHWithdrawalFinalized'
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
            coalesce(decoded_log:"from", decoded_log:"_from") as depositor,
            coalesce(decoded_log:"to", decoded_log:"_to") as recipient,
            coalesce(decoded_log:"amount", decoded_log:"_amount") as amount,
            null as fee,
            decoded_log:"l1Token" as token_address,
            'ethereum' as source_chain,
            'optimism' as destination_chain
        from ethereum_flipside.core.ez_decoded_event_logs
        where
            contract_address = lower('0x99C9fc46f92E8a1c0deC1b1747d010903E884bE1')
            and event_name = 'ERC20DepositInitiated'
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
            coalesce(decoded_log:"from", decoded_log:"_from") as depositor,
            coalesce(decoded_log:"to", decoded_log:"_to") as recipient,
            coalesce(decoded_log:"amount", decoded_log:"_amount") as amount,
            null as fee,
            decoded_log:"l1Token" as token_address,
            'optimism' as source_chain,
            'ethereum' as destination_chain
        from ethereum_flipside.core.ez_decoded_event_logs
        where
            contract_address = lower('0x99C9fc46f92E8a1c0deC1b1747d010903E884bE1')
            and event_name = 'ERC20WithdrawalFinalized'
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
