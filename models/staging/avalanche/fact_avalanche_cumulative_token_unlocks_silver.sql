with recursive
    full_allocation_table as (
        select date, allocation_type, unlock_amount_native
        from {{ ref("fact_avalanche_token_unlocks_silver") }}
    ),
    allocation_type_start_dates as (
        select allocation_type, min(date) as date
        from full_allocation_table
        group by allocation_type
    ),
    date_series as (
        select date, allocation_type
        from allocation_type_start_dates
        union all
        select dateadd(day, 1, date) as date, allocation_type
        from date_series
        where date <= to_date(sysdate())
    ),
    cumulative_by_allocation_type as (
        select
            date,
            allocation_type,
            sum(unlock_amount_native) over (
                partition by allocation_type order by date
            ) as amount_unlocked
        from full_allocation_table
    ),
    full_table as (
        select
            t1.date,
            t1.allocation_type,
            last_value(amount_unlocked ignore nulls) over (
                partition by t1.allocation_type
                order by t1.date
                rows between unbounded preceding and current row
            ) as amount_unlocked_native
        from date_series t1
        left join
            cumulative_by_allocation_type t2
            on t1.date = t2.date
            and t1.allocation_type = t2.allocation_type
    )
select date, 'avalanche' as chain, allocation_type, amount_unlocked_native
from full_table
order by date desc
