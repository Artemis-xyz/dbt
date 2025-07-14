{{
    config(
        materialized="incremental",
        unique_key=["transaction_hash", "transfer_index", "transfer_event"],
        snowflake_warehouse="APTOS_LG",
    )
}}

with deposit_events AS (
    select 
        block_number
        , tx_hash
        , block_timestamp
        , event_index
        , account_address
        , amount
        , token_address
        , transfer_event
    from aptos_flipside.core.fact_transfers
    where transfer_event = 'DepositEvent'
        {% if is_incremental() %}
            and block_timestamp > (select dateadd('day', -3, max(transaction_timestamp)) from {{ this }})
        {% endif %}
)

, withdraw_events AS (
    select 
        block_number
        , tx_hash
        , block_timestamp
        , event_index
        , account_address
        , amount
        , token_address
        , transfer_event
    from aptos_flipside.core.fact_transfers
    where transfer_event = 'WithdrawEvent'
        {% if is_incremental() %}
            and block_timestamp > (select dateadd('day', -3, max(transaction_timestamp)) from {{ this }})
        {% endif %}
)

, unioned AS (
    select * from deposit_events
    union all
    select * from withdraw_events
)

select
    block_timestamp as transaction_timestamp
    , DATE(block_timestamp) as date_day
    , block_number
    , event_index as transfer_index
    , transfer_event
    , tx_hash as transaction_hash
    , account_address
    , amount
    , case 
        when substr(ca.chain_agnostic_id, 0, 7) = 'eip155:' then lower(ca.chain_agnostic_id || ':' || replace(replace(token_address, '0x', ''), '0:', '')) 
        else ca.chain_agnostic_id || ':' || replace(replace(token_address, '0x', ''), '0:', '') 
    end as asset_id
    , 'aptos' as chain_name
    , ca.chain_agnostic_id as chain_id
from unioned
left join {{ ref("chain_agnostic_ids") }} ca
    on 'aptos' = ca.chain