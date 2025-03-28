{% macro stargate_veSTGholders(chain) %} 

with veSTG_txns as (
    select
        ezd.tx_hash,
        ezd.block_timestamp,
        decoded_log:"provider"::STRING AS from_address,
        TRY_CAST(decoded_log:"locktime"::STRING AS NUMBER) AS locktime,
        TRY_CAST(decoded_log:"ts"::STRING AS NUMBER) AS ts,
        TRY_CAST(decoded_log:"value"::STRING AS NUMERIC) / 1e18 AS value
    from {{chain}}_flipside.core.ez_decoded_event_logs ezd
    where 
        topic_0 = '0xbe9cf0e939c614fad640a623a53ba0a807c8cb503c4c4c8dacabe27b86ff2dd5'
        and tx_succeeded = true
        and event_name = 'Deposit'
        and contract_name = 'veSTG'
),
increased_locktime AS (
    select
        from_address,
        max(locktime) as latest_locktime,
        max(ts) as latest_ts
    from veSTG_txns
    where value = 0
    group by from_address
),
data_pre AS (
    select
        v.from_address,
        v.locktime,
        v.ts,
        v.value
    from veSTG_txns v
    left join increased_locktime il 
        on v.from_address = il.from_address
    order by locktime desc
),
data_grouped AS (
    select 
        locktime,
        from_address,
        array_agg(value) as current_array
    from data_pre 
    group by locktime, from_address
    order by locktime desc
),
sum_prev_locked AS (
    select
        locktime,
        from_address,
        current_array,
        case 
            when array_contains(0, current_array) 
            then reduce(lag(current_array, 1) over (partition by from_address order by locktime), 0, (acc, val) -> acc + val)
            else 0
        end as prev_locked_sum
    from data_grouped
),
flatten_prev_locked AS (
    select 
        from_address,
        max(locktime) AS max_locktime
    from sum_prev_locked, lateral flatten(current_array)
    where not array_contains(0, current_array)
    group by from_address
),
final_sum_prev_locked AS (
    select 
        sum_prev_locked.from_address,
        sum_prev_locked.locktime,
        sum_prev_locked.current_array
    from sum_prev_locked
    left join flatten_prev_locked 
        on lower(sum_prev_locked.from_address) = lower(flatten_prev_locked.from_address) 
        and sum_prev_locked.locktime >= flatten_prev_locked.max_locktime
    where locktime >= max_locktime 
),
lateral_flatten AS (
    select
        from_address,
        value,
        locktime
    from final_sum_prev_locked,
    lateral flatten (input => current_array)
),
final_dataset as (
    select 
        from_address, 
        sum(value) as value, 
        max(locktime) as maxlocktime
    from lateral_flatten
    group by from_address
    order by value desc
),
veSTG_locked as (
    select
        from_address,
        maxlocktime as locktime,
        value as stg_locked
    from final_dataset
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
        max(floor(greatest(0, (v.locktime - n.now_ts) / 86400))) as remaining_days,
        max(
            concat(
                floor(greatest(0, (v.locktime - n.now_ts) / 86400) / 365), ' years, ',
                floor(mod(greatest(0, (v.locktime - n.now_ts) / 86400), 365) / 30), ' months, ',
                mod(mod(greatest(0, (v.locktime - n.now_ts) / 86400), 365), 30), ' days'
            )
        ) as remaining_period_readable
    from veSTG_locked v, now_epoch n
    group by from_address
),
stg_balances AS (
    SELECT 
        address AS from_address, 
        balance_token / 1e18 AS stg_balance,
        ROW_NUMBER() OVER (PARTITION BY address ORDER BY block_timestamp DESC) AS rn
    FROM pc_dbt_db.prod.fact_ethereum_address_balances_by_token
    WHERE LOWER(contract_address) = LOWER('0xAf5191B0De278C7286d6C7CC6ab6BB8A73bA2Cd6')
),
current_stg AS (
    SELECT from_address, GREATEST(0, stg_balance) AS stg_balance
    FROM stg_balances WHERE rn = 1
),
voting_data as (
    select 
        lower(address) as from_address,
        count(*) as number_of_votes,
        max(to_timestamp(timestamp)) as last_voted_timestamp
    from landing_database.prod_landing.raw_stargate_proposals_csv
    group by lower(address)
),
last_actions as (
    select 
        from_address, 
        block_timestamp as last_change_timestamp, 
        event_name as last_action_type
    from (
        select decoded_log:"provider"::STRING AS from_address, block_timestamp, event_name,
               row_number() over (partition by decoded_log:"provider"::STRING order by block_timestamp desc) as rn
        from {{chain}}_flipside.core.ez_decoded_event_logs
        where event_name = 'Deposit'
          and tx_succeeded = true
          and topic_0 = '0xbe9cf0e939c614fad640a623a53ba0a807c8cb503c4c4c8dacabe27b86ff2dd5'
    ) where rn = 1
),
circulating_supply as (
    select try_cast(decoded_log:"supply"::STRING AS NUMERIC)/1e18 AS total_supply
    from {{chain}}_flipside.core.ez_decoded_event_logs
    where lower(contract_address) = lower('0x0e42acbd23faee03249daff896b78d7e79fbd58e')
      and tx_succeeded = true 
      and event_name = 'Supply'
    order by block_timestamp desc
    limit 1
),
fees_received as (
    select sum(fees) as total_fees
    from PC_DBT_DB.PROD.fact_stargate_v2_transfers
    where src_chain = '{{chain}}'
),
summary as (
    select 
        v.from_address,
        coalesce(s.stg_balance, 0) as stg_balance,
        v.veSTG_balance,
        v.remaining_days,
        v.remaining_period_readable,
        coalesce(vi.number_of_votes, 0) as number_of_votes_cast,
        vi.last_voted_timestamp,
        la.last_change_timestamp,
        la.last_action_type,
        v.veSTG_balance / nullif(cs.total_supply, 0) * 100 as percentage_of_total_supply,
        fr.total_fees * (1.0/6.0) * (v.veSTG_balance / nullif(vt.total_veSTG, 0)) as fees_received
    from veSTG_balances v
    join (select sum(veSTG_balance) as total_veSTG from veSTG_balances where veSTG_balance > 0 and remaining_days > 0) vt on true
    left join current_stg s on v.from_address = s.from_address
    left join voting_data vi on lower(v.from_address) = vi.from_address
    left join last_actions la on v.from_address = la.from_address
    cross join circulating_supply cs
    cross join fees_received fr
    where v.veSTG_balance > 0 and v.remaining_days > 0
)
select 
    from_address,
    stg_balance,
    veSTG_balance,
    remaining_days,
    remaining_period_readable as remaining_staking_period,
    percentage_of_total_supply,
    number_of_votes_cast,
    last_voted_timestamp,
    last_change_timestamp,
    last_action_type,
    fees_received,
    '{{chain}}' as chain
from summary
order by veSTG_balance desc

{% endmacro %}