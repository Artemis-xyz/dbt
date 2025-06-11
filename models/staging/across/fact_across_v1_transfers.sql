{{
    config(
        materialized="table",
        unique_key=["tx_hash", "event_index"],
        snowflake_warehouse="BRIDGE_MD",
    )
}}
with
    v1_contracts as (
        select '0x02fbb64517e1c6ed69a6faa3abf37db0482f1152' as contract_address
        union
        select '0x43298f9f91a4545df64748e78a2c777c580573d6' as contract_address
        union
        select '0x43f133fe6fdfa17c417695c476447dc2a449ba5b' as contract_address
        union
        select '0x7355efc63ae731f584380a9838292c7046c1e433' as contract_address
        union
        select '0xdfe0ec39291e3b60aca122908f86809c9ee64e90' as contract_address
        union
        select '0x4841572daa1f8e4ce0f62570877c2d0cc18c9535' as contract_address
        union
        select '0x256c8919ce1ab0e33974cf6aa9c71561ef3017b6' as contract_address
    ),

    deposit_relays as (
        select
            contract_address,
            block_timestamp,
            tx_hash,
            event_index,
            decoded_log:"depositData":"amount"::double as amount,
            decoded_log:"depositData":"l2Sender"::string as depositor,
            decoded_log:"depositData":"l1Recipient"::string as recipient,
            1 as destination_chain_id,
            '' as destination_token,
            decoded_log:"depositData":"chainId"::integer as origin_chain_id,
            decoded_log:"relay":"realizedLpFeePct"::integer
            / 1e18 as realized_lp_fee_pct,
            decoded_log:"depositData":"instantRelayFeePct"::integer / 1e18
            + decoded_log:"depositData":"slowRelayFeePct"::integer
            / 1e18 as relayer_fee_pct
        from ethereum_flipside.core.ez_decoded_event_logs
        where
            contract_address in (select contract_address from v1_contracts)
            and event_name = 'DepositRelayed'

            {% if is_incremental() %}

                and block_timestamp
                >= (select dateadd('day', -3, max(block_timestamp)) from {{ this }})

            {% endif %}
    ),

    call_l1_token as (
        select
            to_address,
            concat('0x', substr(output, 27)) as token_address,
            count(*) as count
        from ethereum_flipside.core.fact_traces
        where
            to_address in (select contract_address from v1_contracts)
            and input = '0xc01e1bd6'  -- l1Token call 
        group by 1, 2
    )

select
    contract_address,
    block_timestamp,
    tx_hash,
    event_index,
    amount,
    depositor,
    recipient,
    destination_chain_id,
    b.token_address as destination_token,
    origin_chain_id,
    realized_lp_fee_pct,
    relayer_fee_pct
from deposit_relays a
inner join call_l1_token b on a.contract_address = b.to_address
