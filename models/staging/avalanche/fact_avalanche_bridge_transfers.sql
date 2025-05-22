{{
    config(
        materialized="incremental",
        unique_key=["tx_hash", "event_index"],
        snowflake_warehouse="BRIDGE_MD",
    )
}}

with
    erc20_deposits as (
        with
            txs as (
                select distinct tx_hash
                from avalanche_flipside.core.fact_transactions
                where
                    from_address in (
                        lower('0xEb1bB70123B2f43419d070d7fDE5618971cc2F8f'),
                        lower('0x50Ff3B278fCC70ec7A9465063d68029AB460eA04')
                    )
                    and TX_SUCCEEDED = 1
                    {% if is_incremental() %}

                        and block_timestamp >= (
                            select dateadd('day', -3, max(block_timestamp))
                            from {{ this }}
                        )

                    {% endif %}
            )

        select
            block_timestamp,
            tx_hash,
            event_index,
            decoded_log:"to" as depositor,
            decoded_log:"to" as recipient,
            decoded_log:"amount" + decoded_log:"feeAmount" as amount,
            decoded_log:"feeAmount" as fee,
            contract_address as token_address,
            'ethereum' as source_chain,
            'avalanche' as destination_chain
        from avalanche_flipside.core.ez_decoded_event_logs
        where
            tx_hash in (select * from txs) and event_name = 'Mint'
            {% if is_incremental() %}

                and block_timestamp
                >= (select dateadd('day', -3, max(block_timestamp)) from {{ this }})

            {% endif %}
    ),

    erc20_withdraws as (
        with tokens as (select distinct token_address from erc20_deposits)

        select
            block_timestamp,
            tx_hash,
            event_index,
            decoded_log:"from" as depositor,
            decoded_log:"from" as recipient,
            decoded_log:"value" as amount,
            0 as fee,
            contract_address as token_address,
            'avalanche' as source_chain,
            'ethereum' as destination_chain
        from avalanche_flipside.core.ez_decoded_event_logs
        where
            contract_address in (select * from tokens)
            and event_name = 'Transfer'
            and decoded_log:"to" = '0x0000000000000000000000000000000000000000'
            {% if is_incremental() %}

                and block_timestamp
                >= (select dateadd('day', -3, max(block_timestamp)) from {{ this }})

            {% endif %}
    ),

    bitcoin_deposits as (
        with
            txs as (
                select distinct tx_hash
                from avalanche_flipside.core.fact_transactions
                where
                    from_address
                    in (lower('0xF5163f69F97B221d50347Dd79382F11c6401f1a1'))
                    and TX_SUCCEEDED = 1
            )

        select
            block_timestamp,
            tx_hash,
            event_index,
            decoded_log:"to" as depositor,
            decoded_log:"to" as recipient,
            decoded_log:"amount" as amount,
            decoded_log:"feeAmount" as fee,
            contract_address as token_address,
            'bitcoin' as source_chain,
            'avalanche' as destination_chain
        from avalanche_flipside.core.ez_decoded_event_logs
        where
            tx_hash in (select * from txs) and event_name = 'Mint'
            {% if is_incremental() %}

                and block_timestamp
                >= (select dateadd('day', -3, max(block_timestamp)) from {{ this }})

            {% endif %}
    ),

    bitcoin_withdraws as (
        with tokens as (select distinct token_address from bitcoin_deposits)

        select
            block_timestamp,
            tx_hash,
            event_index,
            decoded_log:"from" as depositor,
            decoded_log:"from" as recipient,
            decoded_log:"value" as amount,
            0 as fee,
            contract_address as token_address,
            'avalanche' as source_chain,
            'bitcoin' as destination_chain
        from avalanche_flipside.core.ez_decoded_event_logs
        where
            contract_address in (select * from tokens)
            and event_name = 'Transfer'
            and decoded_log:"to" = '0x0000000000000000000000000000000000000000'
            {% if is_incremental() %}

                and block_timestamp
                >= (select dateadd('day', -3, max(block_timestamp)) from {{ this }})

            {% endif %}
    )

select *
from erc20_deposits
union
select *
from erc20_withdraws
union
select *
from bitcoin_deposits
union
select *
from bitcoin_withdraws
