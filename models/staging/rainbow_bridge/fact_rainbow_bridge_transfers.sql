{{ config(
    materialized="incremental",
    snowflake_warehouse="RAINBOW_BRIDGE",
) }}
with 
    rainbow_bridge_transfers as (
        select
            block_timestamp,
            tx_hash,
            event_index,
            origin_from_address as depositor,
            origin_from_address as recipient, -- assume it is the same person recievied the token on the other end (may need to adjust)
            coalesce(decoded_log:"amount"::bigint, decoded_log:"value"::bigint, decoded_log:"wad"::bigint, decoded_log:"_value"::bigint, decoded_log:"_amount"::bigint, decoded_log:"tokens"::bigint)  as amount,
            contract_address as token_address,
            'ethereum' as source_chain,
            'near' as destination_chain,
            decoded_log
        from ethereum_flipside.core.ez_decoded_event_logs
        where origin_to_address='0x23ddd3e3692d1861ed57ede224608875809e127f'
            and origin_function_signature='0x0889bfe7'
            and event_name='Transfer'
            {% if is_incremental() %}
                and block_timestamp > (select max(block_timestamp) from {{ this }})
            {% endif %}
        union all
        select 
            block_timestamp,
            tx_hash,
            event_index,
            origin_from_address as depositor,
            origin_from_address as recipient, -- assume it is the same person recievied the token on the other end (may need to adjust)
            coalesce(decoded_log:"amount"::bigint, decoded_log:"value"::bigint, decoded_log:"wad"::bigint, decoded_log:"_value"::bigint, decoded_log:"_amount"::bigint, decoded_log:"tokens"::bigint)  as amount,
            contract_address as token_address,
            'near' as source_chain,
            'ethereum' as destination_chain,
            decoded_log
        from ethereum_flipside.core.ez_decoded_event_logs
        where origin_to_address='0x23ddd3e3692d1861ed57ede224608875809e127f'
            and origin_function_signature='0x4a00c629'
            and event_name='Transfer'
            {% if is_incremental() %}
                and block_timestamp > (select max(block_timestamp) from {{ this }})
            {% endif %}
    ),
    ethereum_to_near_recipient as (
        select
            tx_hash,
            decoded_log:"accountId"::string as depositor
        from ethereum_flipside.core.ez_decoded_event_logs
        where origin_to_address='0x23ddd3e3692d1861ed57ede224608875809e127f'
            and origin_function_signature='0x0889bfe7'
            and event_name='Locked'
            {% if is_incremental() %}
                and block_timestamp > (select max(block_timestamp) from {{ this }})
            {% endif %}
    )

select 
    block_timestamp,
    t.tx_hash,
    event_index,
    coalesce(r.depositor, t.depositor) as depositor,
    recipient,
    t.token_address,
    amount,
    coalesce((amount / power(10, p.decimals)) * price, 0) as amount_usd,
    source_chain,
    destination_chain,
    decoded_log
from rainbow_bridge_transfers t
left join ethereum_to_near_recipient r  on t.tx_hash = r.tx_hash
left join ethereum_flipside.price.ez_hourly_token_prices p
    on date_trunc('hour', t.block_timestamp) = p.hour
    and lower(t.token_address) = lower(p.token_address)