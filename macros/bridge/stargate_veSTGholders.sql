{% macro stargate_veSTGholders(chain, token_contract_address, veSTG_contract_address) %} 

with stg_holders as (
    select 
        tx_hash, 
        from_address as address,
       raw_amount / 1e18 as stg_balance
    from {{chain}}_flipside.core.fact_token_transfers
    where 
        lower(to_address) = lower('{{veSTG_contract_address}}') -- veSTG contract
        and lower(contract_address) = lower('{{token_contract_address}}') -- STG token
),
veSTG_deposits AS (
    select distinct
        ezd.tx_hash,
        ezd.block_timestamp,
        decoded_log:"provider"::STRING as from_address,
        try_cast(decoded_log:"locktime"::STRING as NUMBER) as locktime,
        try_cast(decoded_log:"ts"::STRING as NUMBER) as ts,
        try_cast(decoded_log:"value"::STRING as NUMERIC)/1e18 as value,
        case 
            when locktime is not null then to_timestamp(locktime)
            else null 
        end as locktime_ts,
        case 
            when ts is not null then to_timestamp(ts)
            else null 
        end as deposit_ts,
        case 
            when locktime is not null then greatest(0, datediff(day, current_timestamp(), TO_TIMESTAMP(locktime)))
            else 0
        end as remaining_days,
        case 
            when locktime is not null then
            concat(
              floor(greatest(0, datediff(day, current_timestamp(), TO_TIMESTAMP(locktime))) / 365), ' years, ',
              floor(mod(greatest(0, datediff(day, current_timestamp(), TO_TIMESTAMP(locktime))), 365) / 30), ' months, ',
              mod(greatest(0, datediff(day, current_timestamp(), TO_TIMESTAMP(locktime))), 30), ' days'
            )
            else '0 days'
        end as remaining_stake_readable,
        case 
          when value is null or locktime is null then 0
          when (locktime - extract(epoch from current_timestamp())) <= 0 then 0
          when (locktime - extract(epoch from current_timestamp())) > 94608000 then 0
          else (value * (locktime - extract(epoch from current_timestamp())) / 94608000)
        end as veSTG_balance
    from {{chain}}_flipside.core.ez_decoded_event_logs ezd
    left join stg_holders s on ezd.tx_hash = s.tx_hash
    where topic_0 = '0xbe9cf0e939c614fad640a623a53ba0a807c8cb503c4c4c8dacabe27b86ff2dd5'
        and tx_succeeded = true 
        and event_name = 'Deposit'
        and try_cast(decoded_log:"value"::STRING as NUMERIC) > 0
        and contract_name = 'veSTG'
),
veSTG_withdraws as (
    select
        ezd.tx_hash,
        ezd.block_timestamp,
        decoded_log:"provider"::STRING as from_address,
        -- Safe numeric casting for withdrawal value
        try_cast(decoded_log:"value"::STRING as NUMERIC)/1e18 as withdrawn_value
    from {{chain}}_flipside.core.ez_decoded_event_logs ezd
    where topic_0 = '0xf279e6a1f5e320cca91135676d9cb6e44ca8a08c0b88342bcdb1144f6511b568'
        and tx_succeeded = true 
        and event_name = 'Withdraw'
        and try_cast(decoded_log:"value"::STRING as NUMERIC) > 0
),
stake_events as (
    select
        decoded_log:"provider"::STRING as from_address,
        block_timestamp,
        event_name
    from {{chain}}_flipside.core.ez_decoded_event_logs
    where event_name in ('Deposit', 'Withdraw')
        and tx_succeeded = true
        and topic_0 in ('0xbe9cf0e939c614fad640a623a53ba0a807c8cb503c4c4c8dacabe27b86ff2dd5', '0xf279e6a1f5e320cca91135676d9cb6e44ca8a08c0b88342bcdb1144f6511b568')
),
ranked_stake_changes as (
    select
        from_address,
        block_timestamp,
        event_name,
        row_number() over (
            partition by from_address 
            order by block_timestamp desc
        ) as rn
    from stake_events
),
last_stake_changes as (
    select
        from_address,
        block_timestamp as last_change_timestamp,
        event_name as last_action_type
    from ranked_stake_changes
    where rn = 1
),
-- voting_data as (
--     select 
--         lower(ADDRESS) as from_address,
--         count(*) as number_of_votes,
--         max(TO_TIMESTAMP(TIMESTAMP)) as last_voted_timestamp
--     from landing_database.prod_landing.raw_stargate_proposals_snapshot
--     group by lower(ADDRESS)
-- ),
snapshot_data as (
    select 
        parse_json(source_json) as data,
        regexp_substr(source_url, '/proposal/([a-f0-9x]+)', 1, 1, 'e', 1) as proposal_id
    from landing_database.prod_landing.raw_stargate_proposals_snapshot
),
voting_extraction as (
    select
        proposal_id,
        f.value:id::string as voter_id,
        f.value:voter::string as from_address,
        f.value:choice::number as choice,
        to_timestamp(f.value:created::number) as timestamp,
        to_date(to_timestamp(f.value:created::number)) as date, 
        f.value:vp::double as voting_power
    from snapshot_data, lateral flatten(input => data) f
),
voting_data as (
    select
        lower(from_address) as from_address,
        count(*) as number_of_votes,
        max(timestamp) as last_voted_timestamp
    from voting_extraction
    group by lower(from_address)
),
circulating_supply AS (
    select
        block_timestamp,
        try_cast(decoded_log:"supply"::STRING as NUMERIC)/1e18 AS current_total_supply
    from ethereum_flipside.core.ez_decoded_event_logs
    where lower(contract_address) = lower('0x0e42acbd23faee03249daff896b78d7e79fbd58e')
        and tx_succeeded = true 
        and event_name = 'Supply'
    order by block_timestamp desc
    limit 1
),
-- net_positions AS (
--     select 
--         d.from_address,
--         d.veSTG_balance,
--         d.remaining_days,
--         d.remaining_stake_readable,
--         coalesce(w.total_withdrawn, 0) as total_withdrawn,
--         case 
--             when d.remaining_days = 0 then 0 -- Expired positions have 0 value
--             else greatest(0, d.veSTG_balance - coalesce(w.total_withdrawn, 0))
--         end as net_veSTG_balance
--     from veSTG_deposits d
--     left join (
--         select 
--             from_address, 
--             sum(withdrawn_value) as total_withdrawn
--         from veSTG_withdraws
--         where withdrawn_value is not null
--         group by from_address
--     ) w on d.from_address = w.from_address
--     where d.veSTG_balance > 0
--         and d.remaining_days > 0
-- ),
net_positions AS (
    select 
        d.from_address,
        d.veSTG_balance,
        d.remaining_days,
        d.remaining_stake_readable,
        d.veSTG_balance as net_veSTG_balance
    from veSTG_deposits d
    where d.veSTG_balance > 0
      and d.remaining_days > 0
),
all_holders AS (
    select 
        from_address, 
        sum(net_veSTG_balance) as veSTG_balance,
        max(remaining_days) as max_remaining_days,
        max(remaining_stake_readable) as max_remaining_stake_readable
    from net_positions
    group by from_address
    having sum(net_veSTG_balance) > 0
),
latest_stg_balances as (
    select 
        address as from_address,
        balance_token / 1e18 AS stg_balance,
        row_number() over (
            partition by address 
            order by block_timestamp desc
        ) AS rn
    from {{ ref("fact_"~ chain ~"_address_balances_by_token")}}
    where lower(contract_address) = lower('{{token_contract_address}}') -- STG token
),
current_stg_balances as (
    select
        from_address,
        case 
            when stg_balance < 0 then 0
            else stg_balance
        end as stg_balance
    from latest_stg_balances
    where rn = 1
),
total_veSTG AS (
    select sum(veSTG_balance) as total_veSTG_amount
    from all_holders
),
fees_received as (
    select
        sum(fees) as total_fees
    from PC_DBT_DB.PROD.fact_stargate_v2_transfers
    where src_chain = '{{chain}}'
),
top_holders AS (
    select 
        h.from_address, 
        coalesce(s.stg_balance, 0) as stg_balance,
        h.veSTG_balance,
        h.max_remaining_days as remaining_days,
        h.max_remaining_stake_readable as remaining_staking_period,
        coalesce(v.number_of_votes, 0) as number_of_votes_cast,
        v.last_voted_timestamp,
        l.last_change_timestamp,
        l.last_action_type,
        case 
            when c.current_total_supply > 0 then (h.veSTG_balance / nullif(c.current_total_supply, 0)) * 100
            else 0
        end AS percentage_of_circulating_supply,
        case
            when (select total_veSTG_amount from total_veSTG) > 0 then
                (select total_fees from fees_received) * (1.0/6.0) * 
                (h.veSTG_balance / nullif((select total_veSTG_amount from total_veSTG), 0))
            else 0
        end as fees_received
    from all_holders h
    left join current_stg_balances s on h.from_address = s.from_address
    left join voting_data v on lower(h.from_address) = v.from_address
    left join last_stake_changes l on h.from_address = l.from_address
    cross join circulating_supply c
    order by h.veSTG_balance desc
    limit 100
)
select
    from_address,
    stg_balance,
    veSTG_balance,
    remaining_days,
    remaining_staking_period,
    percentage_of_circulating_supply,
    number_of_votes_cast,
    last_voted_timestamp ,
    last_change_timestamp,
    last_action_type,
    fees_received,
    '{{chain}}' as chain
from top_holders
order by veSTG_balance desc

{% endmacro %}