{{
    config(
        materialized="table",
        unique_key=["tx_hash", "event_index"],
        snowflake_warehouse="ARBITRUM",
    )
}}

with
    eth_deposits as (
        select
            block_timestamp,
            tx_hash,
            trace_index as event_index,
            origin_from_address as depositor,
            origin_from_address as recipient,
            amount_precise_raw as amount,
            '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' as token_address,
            'ethereum' as source_chain,
            'arbitrum' as destination_chain
        from ethereum_flipside.core.ez_native_transfers
        where
            to_address = lower('0x8315177aB297bA92A06054cE80a67Ed4DBd7ed3a')
            {% if is_incremental() %}

                and block_timestamp
                >= (select dateadd('day', -3, max(block_timestamp)) from {{ this }})

            {% endif %}
            and inserted_timestamp < to_date(sysdate())
    ),

    eth_withdraws as (
        select
            block_timestamp,
            tx_hash,
            event_index,
            decoded_log:"to" as depositor,
            decoded_log:"to" as recipient,
            decoded_log:"value" as amount,
            '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' as token_address,
            'arbitrum' as source_chain,
            'ethereum' as destination_chain
        from ethereum_flipside.core.ez_decoded_event_logs
        where
            contract_address = lower('0x8315177aB297bA92A06054cE80a67Ed4DBd7ed3a')
            and event_name = 'BridgeCallTriggered'
            and tx_succeeded = TRUE
            {% if is_incremental() %}

                and block_timestamp
                >= (select dateadd('day', -3, max(block_timestamp)) from {{ this }})

            {% endif %}
            and inserted_timestamp < to_date(sysdate())
    ),

    mainnet_gateways as (
        select distinct decoded_log:"gateway" as contract_address
        from ethereum_flipside.core.ez_decoded_event_logs
        -- L1 Gateway Router
        where
            contract_address = lower('0x72Ce9c846789fdB6fC1f34aC4AD25Dd9ef7031ef')
            and event_name = 'TransferRouted'
            and inserted_timestamp < to_date(sysdate())
    ),

    erc20_deposits as (
        select
            block_timestamp,
            tx_hash,
            event_index,
            case
                when decoded_log:"_from" is null
                then decoded_log:"from"
                else decoded_log:"_from"
            end as depositor,
            case
                when decoded_log:"_to" is null
                then decoded_log:"to"
                else decoded_log:"_to"
            end as recipient,
            case
                when decoded_log:"_amount" is null
                then decoded_log:"amount"
                else decoded_log:"_amount"
            end as amount,
            decoded_log:"l1Token" as token_address,
            'ethereum' as source_chain,
            'arbitrum' as destination_chain
        from ethereum_flipside.core.ez_decoded_event_logs
        where
            contract_address in (
                -- exclude WETH Gateway
                select *
                from mainnet_gateways
                where
                    contract_address
                    != lower('0xd92023E9d9911199a6711321D1277285e6d4e2db')
            )
            and event_name = 'DepositInitiated'
            and tx_succeeded = TRUE
            {% if is_incremental() %}

                and block_timestamp
                >= (select dateadd('day', -3, max(block_timestamp)) from {{ this }})

            {% endif %}
            and inserted_timestamp < to_date(sysdate())
    ),

    arbitrum_one_gateways as (
        select distinct decoded_log:"gateway" as contract_address
        from arbitrum_flipside.core.ez_decoded_event_logs
        -- L2 Gateway Router
        where
            contract_address = lower('0x5288c571fd7ad117bea99bf60fe0846c4e84f933')
            and event_name = 'TransferRouted'
            and inserted_timestamp < to_date(sysdate())
    ),

    erc20_withdraws as (
        select
            block_timestamp,
            tx_hash,
            event_index,
            case
                when decoded_log:"_from" is null
                then decoded_log:"from"
                else decoded_log:"_from"
            end as depositor,
            case
                when decoded_log:"_to" is null
                then decoded_log:"to"
                else decoded_log:"_to"
            end as recipient,
            case
                when decoded_log:"_amount" is null
                then decoded_log:"amount"
                else decoded_log:"_amount"
            end as amount,
            decoded_log:"l1Token" as token_address,
            'arbitrum' as source_chain,
            'ethereum' as destination_chain
        from arbitrum_flipside.core.ez_decoded_event_logs
        where
            contract_address in (
                -- exclude WETH Gateway
                select *
                from arbitrum_one_gateways
                where
                    contract_address
                    != lower('0x6c411ad3e74de3e7bd422b94a27770f5b86c623b')
            )
            and event_name = 'WithdrawalInitiated'
            and tx_succeeded = TRUE
            {% if is_incremental() %}

                and block_timestamp
                >= (select dateadd('day', -3, max(block_timestamp)) from {{ this }})

            {% endif %}
            and inserted_timestamp < to_date(sysdate())
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
