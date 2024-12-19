{{
    config(
        materialized="incremental",
        snowflake_warehouse="HIVEMAPPER",
        unique_key=["tx_id", "action", "index", "inner_index"]
    )
}}

select
    block_timestamp,
    tx_id,
    index,
    inner_index,
    log_messages,
    action,
    case 
        when ARRAY_TO_STRING(log_messages, ',') like '%Program log: Memo (len 12): "Map Coverage"%' then 'Map Coverage'
        when ARRAY_TO_STRING(log_messages, ',') like 'Program log: Memo (len 20): \"Map Coverage (Fleet)\"' then 'Map Coverage (Fleet)'
        when ARRAY_TO_STRING(log_messages, ',') like '%Program log: Memo (len 6): "Bounty"%' then 'Bounty'
        when ARRAY_TO_STRING(log_messages, ',') like '%Program log: Memo (len 4): "Buzz"%' then 'Buzz'
        when ARRAY_TO_STRING(log_messages, ',') like '%Program log: Memo (len 18): "Map Editing and QA"%' then 'Map Editing and QA'
        when ARRAY_TO_STRING(log_messages, ',') like '%Program log: Memo (len 15): "Map Consumption"%' then 'Map Consumption'
        when ARRAY_TO_STRING(log_messages, ',') like '%Program log: Memo (len 23): "Map Consumption (fleet)"%' then 'Map Consumption (fleet)'
        when ARRAY_TO_STRING(log_messages, ',') like '%Program log: Memo (len 17): "Foundation Reward"%' then 'FTM'
        when ARRAY_TO_STRING(log_messages, ',') like '%Program log: Memo (len 11): "Honey Burst"%' then 'Honey Burst'
        when ARRAY_TO_STRING(log_messages, ',') like '%Program log: Memo (len 19): "Honey Burst (fleet)"%' then 'Honey Burst (fleet)'
        else null
    end as reward_type,
    tx_to_account,
    o1.owner as tx_to_owner,
    tx_from_account,
    o2.owner as tx_from_owner,
    amount_native,
    amount_native * p.price as amount_usd
from {{ref('fact_hivemapper_mints_burns_transfers')}}
left join solana_flipside.core.fact_token_account_owners o1 on o1.account_address = tx_to_account
left join solana_flipside.core.fact_token_account_owners o2 on o2.account_address = tx_from_account
left join solana_flipside.price.ez_prices_hourly p on p.hour = date_trunc('hour', block_timestamp) and p.token_address = '4vMsoUT2BWatFweudnQM1xedRLfJgJ7hswhcpz4xgBTy'
where 1=1
    and (
        not(action = 'transfer' AND reward_type is null)
    )
{% if is_incremental() %}
    and block_timestamp >= (select dateadd('day', -3, max(block_timestamp)) from {{ this }})
{% endif %}