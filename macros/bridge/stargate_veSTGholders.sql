{% macro stargate_veSTGholders(chain) %} 

with veSTG_txns as (
    select
        ezd.tx_hash,
        ezd.block_timestamp,
        decoded_log:"provider"::STRING AS from_address,
        try_cast(decoded_log:"locktime"::STRING AS NUMBER) AS locktime,
        try_cast(decoded_log:"ts"::STRING AS NUMBER) AS ts,
        try_cast(decoded_log:"value"::STRING AS NUMERIC) / 1e18 AS value
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
        locktime AS latest_locktime,
        ts AS latest_ts
    from (
        select 
            from_address,
            locktime,
            ts,
            row_number() over (partition by from_address order by ts desc) as rn
        from veSTG_txns
        where value = 0
    ) sub
    where rn = 1
),
adjusted_locktime_data AS (
    select
        v.tx_hash,
        v.block_timestamp,
        v.from_address,
        v.locktime,
        v.ts,
        v.value,
        case 
            when v.value != 0 and il.latest_ts < v.locktime 
            then il.latest_locktime
            else v.locktime
        end as adjusted_next_locktime,
        v.value as stg_amount
    from veSTG_txns v
    left join increased_locktime il 
        on v.from_address = il.from_address
),
current_time AS (
    select current_timestamp() as now_ts
),
now_epoch as (select extract(epoch from current_timestamp()) as now_ts
),
veSTG_balances as (
    select 
        from_address,
        sum(case when adjusted_next_locktime > n.now_ts then value else 0 end) as stg_locked,
        sum(case when adjusted_next_locktime > n.now_ts then value * (adjusted_next_locktime - n.now_ts) / 94608000 else 0 end) as veSTG_balance,
        max(floor(greatest(0, (adjusted_next_locktime - n.now_ts) / 86400))) as remaining_days,
        max(floor(greatest(0, (adjusted_next_locktime - n.now_ts) / 86400)) / 365) as years,
        max(floor(mod(greatest(0, (adjusted_next_locktime - n.now_ts) / 86400), 365) / 30)) as months,
        max(mod(mod(greatest(0, (adjusted_next_locktime - n.now_ts) / 86400), 365), 30)) as days,
        max(
            concat(
                floor(greatest(0, (adjusted_next_locktime - n.now_ts) / 86400) / 365), ' years, ',
                floor(mod(greatest(0, (adjusted_next_locktime - n.now_ts) / 86400), 365) / 30), ' months, ',
                mod(mod(greatest(0, (adjusted_next_locktime - n.now_ts) / 86400), 365), 30), ' days'
            )
        ) as remaining_period_readable
    from adjusted_locktime_data, now_epoch n
    group by from_address
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
    select from_address, block_timestamp as last_change_timestamp, event_name as last_action_type
    from (
        select decoded_log:"provider"::STRING as from_address, block_timestamp, event_name,
               row_number() over (partition by decoded_log:"provider"::STRING order by block_timestamp desc) as rn
        from ethereum_flipside.core.ez_decoded_event_logs
        where event_name = 'Deposit'
          and tx_succeeded = true
          and topic_0 = '0xbe9cf0e939c614fad640a623a53ba0a807c8cb503c4c4c8dacabe27b86ff2dd5'
    ) where rn = 1
),
circulating_supply as (
    select try_cast(decoded_log:"supply"::STRING as NUMERIC)/1e18 as total_supply
    from ethereum_flipside.core.ez_decoded_event_logs
    where lower(contract_address) = lower('0x0e42acbd23faee03249daff896b78d7e79fbd58e')
      and tx_succeeded = true 
      and event_name = 'Supply'
    order by block_timestamp desc limit 1
),
fees_received as (
    select sum(fees) as total_fees
    from PC_DBT_DB.PROD.fact_stargate_v2_transfers
    where src_chain = 'ethereum'
),
stg_balances as (
    select address as from_address, balance_token / 1e18 as stg_balance,
           row_number() over (partition by address order by block_timestamp desc) as rn
    from pc_dbt_db.prod.fact_ethereum_address_balances_by_token
    where lower(contract_address) = lower('0xAf5191B0De278C7286d6C7CC6ab6BB8A73bA2Cd6')
),
current_stg as (
    select from_address, greatest(0, stg_balance) as stg_balance
    from stg_balances where rn = 1
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
        v.veSTG_balance / nullif(cs.total_supply, 0) * 100 as percentage_of_circulating_supply,
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
    percentage_of_circulating_supply,
    number_of_votes_cast,
    last_voted_timestamp,
    last_change_timestamp,
    last_action_type,
    fees_received,
    '{{chain}}' as chain
from summary
order by veSTG_balance desc
limit 100

{% endmacro %}