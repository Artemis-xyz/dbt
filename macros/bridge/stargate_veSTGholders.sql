{% macro stargate_veSTGholders(chain, token_contract_address) %} 

with veSTG_txns as (
    select
        ezd.tx_hash,
        ezd.block_timestamp,
        decoded_log:"provider"::string AS from_address,
        try_cast(decoded_log:"locktime"::string AS number) AS locktime,
        try_cast(decoded_log:"ts"::string AS number) AS ts,
        try_cast(decoded_log:"value"::string AS numeric) / 1e18 AS value
    from {{chain}}_flipside.core.ez_decoded_event_logs ezd
    where 
        topic_0 = '0xbe9cf0e939c614fad640a623a53ba0a807c8cb503c4c4c8dacabe27b86ff2dd5'
        and tx_succeeded = true
        and event_name = 'Deposit'
        and contract_name = 'veSTG'
),
STG_locked_array_agg_values as (
    select 
        locktime,
        from_address,
        array_agg(value) as array_agg_value
    from veSTG_txns 
    group by locktime, from_address
    order by locktime desc
),
STG_locked_sum_prev_locked as (
    select
        locktime,
        from_address,
        array_agg_value,
        case 
            when array_contains(0, array_agg_value) 
            then reduce(lag(array_agg_value, 1) over (partition by from_address order by locktime), 0, (acc, val) -> acc + val)
            else 0
        end as prev_locked_sum
    from STG_locked_array_agg_values
),
STG_locked_flatten_prev_locked as (
    select 
        from_address,
        max(locktime) AS max_locktime
    from STG_locked_sum_prev_locked, lateral flatten(input => array_agg_value)
    where not array_contains(0, array_agg_value)
    group by from_address
),
STG_final_locked_sum_prev_locked as (
    select 
        s.from_address,
        s.locktime,
        s.array_agg_value,
    from STG_locked_sum_prev_locked s
    left join STG_locked_flatten_prev_locked f
        on lower(s.from_address) = lower(f.from_address) 
        and s.locktime >= f.max_locktime
    where s.locktime >= f.max_locktime
),
STG_min_ts as (
    select 
        locktime,
        from_address,
        min(ts) as min_ts
    from veSTG_txns
    group by locktime, from_address
),
STG_min_ts_values as (
    select 
        s.from_address,
        s.locktime,
        s.array_agg_value,
        case 
            when v.locktime is not null and lower(s.from_address) = lower(v.from_address) 
            then v.min_ts
            else null
        end as ts
    from STG_final_locked_sum_prev_locked s
    left join STG_min_ts v
        on s.locktime = v.locktime and lower(s.from_address) = lower(v.from_address)
),
STG_lateral_flatten as (
    select
        from_address,
        ts,
        value,
        locktime
    from STG_min_ts_values,
    lateral flatten(input => array_agg_value)
),
STG_maxlocktime as (
    select 
        from_address, 
        sum(value) AS value, 
        max(locktime) AS maxlocktime,
        min(ts) as minTS
    from STG_lateral_flatten
    group by from_address
    order by value desc
),
STG_locked_current_balance as (
    select
        from_address,
        maxlocktime AS locktime,
        value AS stg_locked,
        floor((extract(epoch from current_timestamp()) - minTS) / 86400) as num_days_staked
    from STG_maxlocktime
    where to_timestamp(maxlocktime) > current_timestamp()
),
now_epoch as (
    select extract(epoch from current_timestamp()) as now_ts
),
veSTG_balances as (
    select 
        v.from_address,
        sum(case when v.locktime > n.now_ts then v.stg_locked else 0 end) as stg_locked,
        sum(case when v.locktime > n.now_ts then v.stg_locked * (v.locktime - n.now_ts) / 94608000 else 0 end) as veSTG_balance,
        min(v.num_days_staked) as num_days_staked,
        max(floor(greatest(0, (v.locktime - n.now_ts) / 86400))) as remaining_days,
        max(
            concat(
                floor(greatest(0, (v.locktime - n.now_ts) / 86400) / 365), ' years, ',
                floor(mod(greatest(0, (v.locktime - n.now_ts) / 86400), 365) / 30), ' months, ',
                mod(mod(greatest(0, (v.locktime - n.now_ts) / 86400), 365), 30), ' days'
            )
        ) AS remaining_period_readable
    from STG_locked_current_balance v
    cross join now_epoch n
    group by v.from_address
),
stg_balances as (
    select 
        address as from_address, 
        balance_token / 1e18 as stg_balance,
        row_number() over (partition by address order by block_timestamp desc) as rn
    from {{ ref("fact_" ~ chain ~ "_address_balances_by_token") }}
    where lower(contract_address) = lower('{{token_contract_address}}')
),
current_stg as (
    select 
        from_address, 
        greatest(0, stg_balance) as stg_balance
    from stg_balances
    where rn = 1
),
voting_data as (
    select 
        lower(address) as from_address,
        count(*) as number_of_votes,
        max(to_timestamp(timestamp)) as last_voted_timestamp
    from {{ source("PROD_LANDING", "raw_stargate_proposals_csv") }}
    group by lower(address)
),
last_actions as (
    select 
        from_address, 
        block_timestamp as last_change_timestamp, 
        event_name as last_action_type
    from (
        select 
            decoded_log:"provider"::STRING AS from_address, 
            block_timestamp, 
            event_name,
            row_number() over (partition by decoded_log:"provider"::STRING order by block_timestamp desc) as rn
        from {{chain}}_flipside.core.ez_decoded_event_logs
        where event_name = 'Deposit'
          and tx_succeeded = true
          and topic_0 = '0xbe9cf0e939c614fad640a623a53ba0a807c8cb503c4c4c8dacabe27b86ff2dd5'
    ) 
    where rn = 1
),
summary as (
    select
        v.from_address,
        coalesce(s.stg_balance, 0) as stg_balance,
        v.stg_locked,
        v.veSTG_balance,
        v.remaining_days,
        v.remaining_period_readable,
        coalesce(vi.number_of_votes, 0) as number_of_votes_cast,
        vi.last_voted_timestamp,
        la.last_change_timestamp,
        la.last_action_type,
        v.num_days_staked,
    from veSTG_balances v
    left join current_stg s on v.from_address = s.from_address
    left join voting_data vi on lower(v.from_address) = vi.from_address
    left join last_actions la on v.from_address = la.from_address
)
select 
    from_address,
    stg_balance,
    stg_locked,
    veSTG_balance,
    remaining_days,
    remaining_period_readable as remaining_staking_period,
    number_of_votes_cast,
    last_voted_timestamp,
    last_change_timestamp,
    last_action_type,
    num_days_staked,
    '{{chain}}' as chain
from summary
order by veSTG_balance desc

{% endmacro %}