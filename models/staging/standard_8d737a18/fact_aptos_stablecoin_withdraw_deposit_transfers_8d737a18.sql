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
    u.block_timestamp as transaction_timestamp
    , DATE(u.block_timestamp) as date_day
    , u.block_number
    , u.event_index as transfer_index
    , transactions.version AS transaction_position
    , u.transfer_event
    , u.tx_hash as transaction_hash
    , u.account_address
    , u.amount / pow(10, contracts.num_decimals) AS amount_asset
    , case 
        when substr(ca.chain_agnostic_id, 0, 7) = 'eip155:' then lower(ca.chain_agnostic_id || ':' || replace(replace(u.token_address, '0x', ''), '0:', '')) 
        else ca.chain_agnostic_id || ':' || replace(replace(u.token_address, '0x', ''), '0:', '') 
    end as asset_id
    , contracts.symbol AS asset_symbol
    , 'aptos' as chain_name
    , ca.chain_agnostic_id as chain_id
from unioned u
join {{ref("fact_aptos_stablecoin_contracts")}} contracts
        on lower(u.token_address) = lower(contracts.contract_address)
left join {{ ref("chain_agnostic_ids") }} ca
    on 'aptos' = ca.chain
left join aptos_flipside.core.fact_transactions transactions
    on u.tx_hash = transactions.tx_hash
where lower(u.token_address) in (
    select lower(contract_address)
    from {{ref("fact_aptos_stablecoin_contracts")}}
)