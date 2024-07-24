{% macro address_credits(chain, wrapped_native_token_address, native_token_address) %}
    
    select
        to_address as address,
        contract_address,
        block_timestamp,
        cast(raw_amount as float) as credit,
        amount_usd as credit_usd,
        tx_hash,
        -1 as trace_index,
        event_index
    from {{ chain }}_flipside.core.ez_token_transfers
    where
        to_address <> lower('0x0000000000000000000000000000000000000000')
        and to_date(block_timestamp) < to_date(sysdate())
        {% if is_incremental() %}
            and block_timestamp
            >= (select dateadd('day', -3, max(block_timestamp)) from {{ this }})
        {% endif %}
        
    union all

    select
        to_address as address,
        'native_token' as contract_address,
        block_timestamp,
        amount as credit,
        amount_usd as credit_usd,
        tx_hash,
        trace_index,
        -1 as event_index
    from {{ chain }}_flipside.core.ez_native_transfers
    where
        to_date(block_timestamp) < to_date(sysdate())
        and to_address <> lower('0x0000000000000000000000000000000000000000')
        {% if is_incremental() %}
            and block_timestamp
            >= (select dateadd('day', -3, max(block_timestamp)) from {{ this }})
        {% endif %}
        
    {% if wrapped_native_token_address is defined and chain not in ('polygon')%}
        union all
        select
            decoded_log:"dst"::string as address,
            contract_address,
            block_timestamp,
            cast(decoded_log:"wad" as float) as credit,
            null as credit_usd,
            tx_hash,
            -1 as trace_index,
            event_index
        from {{ chain }}_flipside.core.ez_decoded_event_logs
        where
            event_name = 'Deposit'
            and contract_address = lower('{{ wrapped_native_token_address }}')
            and to_date(block_timestamp) < to_date(sysdate())
            and address <> lower('0x0000000000000000000000000000000000000000')
            {% if is_incremental() %}
                and block_timestamp
                >= (select dateadd('day', -3, max(block_timestamp)) from {{ this }})
            {% endif %}

    {% endif %}
    -- Need to include credits of old bridge deposits of eth to arbitrum. Only need to
    -- do this
    -- for full refreshes since the bridge is no longer used.
    {% if chain == "arbitrum" and not is_incremental() %}
        union all
        select
            origin_to_address as address,
            'native_token' as contract_address,
            block_timestamp,
            amount as credit,
            null as credit_usd,
            tx_hash,
            -1 as trace_index,
            -1 as event_index
        from ethereum_flipside.core.ez_native_transfers
        where to_address = lower('0x011B6E24FfB0B5f5fCc564cf4183C5BBBc96D515')

    {% endif %}

    -- Some EVM Chains has a specific contract address for their native token (polygon) 
    -- When you bridge the native token from ethereum (deposit matic) it does not emit
    -- a transfer event
    -- (neither as a native transfer or a erc20 transfer)
    -- so we need to seperately account for these events.
    {% if native_token_address is defined %}
        union all
        select
            decoded_log:"from"::string as address,
            'native_token' as contract_address,
            block_timestamp,
            cast(decoded_log:"amount" as float) / pow(10, 18) as credit,
            null as credit_usd,
            tx_hash,
            -1 as trace_index,
            event_index
        from {{ chain }}_flipside.core.ez_decoded_event_logs
        where
            event_name = 'Deposit'
            and contract_address = lower('{{ native_token_address }}')
            and to_date(block_timestamp) < to_date(sysdate())
            and address <> lower('0x0000000000000000000000000000000000000000')
            {% if is_incremental() %}
                and block_timestamp
                >= (select dateadd('day', -3, max(block_timestamp)) from {{ this }})
            {% endif %}

    {% endif %}
-- Not included is staking fees (need to do more research and would be chains specific)
{% endmacro %}
