{{ config(snowflake_warehouse="ETHEREUM_XS", materialized="table") }}
with
    grouped_validator_info as (
        select to_date(t2.slot_timestamp) as date, pubkey, validator_status
        from ethereum_flipside.beacon_chain.fact_validators t1
        left join
            ethereum_flipside.beacon_chain.fact_blocks t2
            on t1.slot_number = t2.slot_number
        where
            validator_status in (
                'active_ongoing',
                'active_exiting',
                'active_slashed',
                'pending_queued',
                'exited_unslashed',
                'exited_slashed'
            )
            and t1.slot_number >= 4700013
        group by date, pubkey, validator_status
    )
select
    date,
    'ethereum' as chain,
    sum(
        case when validator_status in ('pending_queued') then 1 else 0 end
    ) as queue_entry_amount,
    sum(
        case
            when validator_status in ('active_ongoing', 'active_slashed') then 1 else 0
        end
    ) as queue_active_amount,
    sum(
        case when validator_status in ('active_exiting') then 1 else 0 end
    ) as queue_exit_amount
from grouped_validator_info
group by date
order by date desc
