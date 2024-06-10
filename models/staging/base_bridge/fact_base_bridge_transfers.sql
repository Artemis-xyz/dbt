{{
    config(
        materialized="incremental",
        unique_key=["tx_hash", "event_index"],
        snowflake_warehouse="BRIDGE_MD",
    )
}}

with
    eth_deposits as (
        with
            deposit_bridge as (
                select
                    block_timestamp,
                    tx_hash,
                    event_index,
                    decoded_log:"from"::string as depositor,
                    decoded_log:"to"::string as recipient,
                    decoded_log:"amount"::bigint as amount,
                    null as fee,
                    '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' as token_address,
                    'ethereum' as source_chain,
                    'base' as destination_chain
                from ethereum_flipside.core.ez_decoded_event_logs
                where
                    contract_address
                    = lower('0x3154Cf16ccdb4C6d922629664174b904d80F2C35')
                    and event_name = 'ETHDepositInitiated'
                    {% if is_incremental() %}

                        and block_timestamp >= (
                            select dateadd('day', -3, max(block_timestamp))
                            from {{ this }}
                        )

                    {% endif %}
            ),

            deposit_portal as (

                select
                    block_timestamp,
                    tx_hash,
                    trace_index as event_index,
                    origin_from_address::string as depositor,
                    origin_from_address::string as recipient,
                    amount_precise_raw::bigint as amount,
                    null as fee,
                    '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' as token_address,
                    'ethereum' as source_chain,
                    'base' as destination_chain
                from ethereum_flipside.core.ez_native_transfers
                where
                    to_address = lower('0x49048044d57e1c92a77f79988d21fa8faf74e97e')
                    and origin_to_address
                    = lower('0x49048044d57e1c92a77f79988d21fa8faf74e97e')
                    {% if is_incremental() %}

                        and block_timestamp >= (
                            select dateadd('day', -3, max(block_timestamp))
                            from {{ this }}
                        )

                    {% endif %}
            )

        select *
        from deposit_bridge
        union
        select *
        from deposit_portal
    ),

    eth_withdraws as (
        with
            bridge_withdraws as (
                select
                    block_timestamp,
                    tx_hash,
                    event_index,
                    decoded_log:"from"::string as depositor,
                    decoded_log:"to"::string as recipient,
                    decoded_log:"amount"::bigint as amount,
                    null as fee,
                    '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' as token_address,
                    'base' as source_chain,
                    'ethereum' as destination_chain
                from ethereum_flipside.core.fact_decoded_event_logs
                where
                    contract_address
                    = lower('0x3154Cf16ccdb4C6d922629664174b904d80F2C35')
                    and event_name = 'ETHWithdrawalFinalized'
                    {% if is_incremental() %}

                        and block_timestamp >= (
                            select dateadd('day', -3, max(block_timestamp))
                            from {{ this }}
                        )

                    {% endif %}
            ),

            portal_withdraws as (
                select
                    block_timestamp,
                    tx_hash,
                    trace_index as event_index,
                    origin_from_address::string as depositor,
                    origin_from_address::string as recipient,
                    amount_precise_raw::bigint as amount,
                    null as fee,
                    '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' as token_address,
                    'ethereum' as source_chain,
                    'base' as destination_chain
                from ethereum_flipside.core.ez_native_transfers
                where
                    origin_from_address
                    = lower('0x49048044d57e1c92a77f79988d21fa8faf74e97e')
                    {% if is_incremental() %}

                        and block_timestamp >= (
                            select dateadd('day', -3, max(block_timestamp))
                            from {{ this }}
                        )

                    {% endif %}
            )

        select *
        from bridge_withdraws
        union
        select *
        from portal_withdraws
    ),

    erc20_deposits as (
        select
            block_timestamp,
            tx_hash,
            event_index,
            decoded_log:"from"::string as depositor,
            decoded_log:"to"::string as recipient,
            decoded_log:"amount"::bigint as amount,
            null as fee,
            decoded_log:"l1Token" as token_address,
            'ethereum' as source_chain,
            'base' as destination_chain
        from ethereum_flipside.core.fact_decoded_event_logs
        where
            contract_address = lower('0x3154Cf16ccdb4C6d922629664174b904d80F2C35')
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
            decoded_log:"from"::string as depositor,
            decoded_log:"to"::string as recipient,
            decoded_log:"amount"::bigint as amount,
            null as fee,
            decoded_log:"l1Token" as token_address,
            'base' as source_chain,
            'ethereum' as destination_chain
        from ethereum_flipside.core.fact_decoded_event_logs
        where
            contract_address = lower('0x3154Cf16ccdb4C6d922629664174b904d80F2C35')
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
