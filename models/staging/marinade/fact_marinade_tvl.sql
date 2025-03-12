{{ config(
    materialized="table",
    snowflake_warehouse="MARINADE"
) }}

with daily_net as (
    select
        date(block_timestamp) AS stake_date,
        sum(
            case 
                when event_type in ('deposit', 'depositStakeAccount')
                    then decoded_args:lamports::numeric / 1e9
                when event_type in ('liquidUnstake', 'orderUnstake')
                    then -decoded_args:msolAmount::numeric / 1e9
                else 0
            end
    ) as net_stake_amount
    from solana_flipside.core.ez_events_decoded
    where lower(program_id) = lower('MarBmsSgKXdrN1egZf5sqe1TMai9K1rChYNDJgjq7aD')
    group by date(block_timestamp)
),
cumulative_deposits as (
    select
        stake_date,
        net_stake_amount,
        sum(
            case when stake_date <= '2021-10-19' and net_stake_amount > 0
                then net_stake_amount
                else 0
            end
        ) over (order by stake_date asc) as cumulative_net_stake_amount
    from daily_net
),
marinade_liquid as (
    select
        stake_date,
        case 
            when stake_date <= '2021-10-19' then cumulative_net_stake_amount
            when stake_date between '2021-10-20' and '2022-07-26' 
                then cumulative_net_stake_amount + net_stake_amount
            else null
        end as net_stake_adjusted
    from cumulative_deposits
),
marinade as (
    select
        stake_date as date,
        net_stake_adjusted as liquid,
        0 as native,
        net_stake_adjusted as tvl
    from marinade_liquid
    where stake_date < '2022-07-27'
),
sol_staked_by_epoch as (
    select
        s.epoch,
        sum(
            case when s.authorized_staker = '4bZ6o3eUUNXhKuqjdCnCoPAoLgWiuLYixKaxoa8PpiKk' 
                then s.active_stake 
                else 0 
            end
        ) as liquid,
        sum(
            case when s.authorized_staker = 'stWirqFCf2Uts1JBL1Jsd3r6VBWhgnpdPxCTe1MFjrq' 
                then s.active_stake 
                else 0 
            end
        ) as native,
        sum(
            case when s.authorized_staker in (
                '4bZ6o3eUUNXhKuqjdCnCoPAoLgWiuLYixKaxoa8PpiKk', 
                'stWirqFCf2Uts1JBL1Jsd3r6VBWhgnpdPxCTe1MFjrq'
            )
                then s.active_stake 
                else 0 
            end
        ) as tvl
    from solana_flipside.gov.fact_stake_accounts s
    where s.authorized_staker in (
        '4bZ6o3eUUNXhKuqjdCnCoPAoLgWiuLYixKaxoa8PpiKk', 
        'stWirqFCf2Uts1JBL1Jsd3r6VBWhgnpdPxCTe1MFjrq'
    )
    group by s.epoch
),
epoch_to_timestamp as (
    select
        e.epoch,
        min(fb.block_timestamp) as epoch_start_timestamp
    from solana_flipside.gov.dim_epoch e
    left join solana_flipside.core.fact_blocks fb 
        on fb.block_id between e.start_block and e.end_block
    where e.epoch >= 330
    group by e.epoch
),
block_data as (
    select 
        s.epoch, 
        etd.epoch_start_timestamp AS date, 
        s.liquid, 
        s.native, 
        s.tvl
    from sol_staked_by_epoch s
    left join epoch_to_timestamp etd 
        on s.epoch = etd.epoch
),
date_spine as (
    select date
    from pc_dbt_db.prod.dim_date_spine
    where date between '2022-07-27' and to_date(sysdate())
),
all_dates as (
    select
        ds.date,
        coalesce(
            last_value(bd.liquid ignore nulls) over (
                order by ds.date 
                rows between unbounded preceding and current row
            ), 0
        ) as liquid,
        coalesce(
            last_value(bd.native ignore nulls) over (
                order by ds.date 
                rows between unbounded preceding and current row
            ), 0
        ) as native,
        coalesce(
            last_value(bd.tvl ignore nulls) over (
                order by ds.date 
                rows between unbounded preceding and current row
            ), 0
        ) as tvl
    from date_spine ds
    left join block_data bd 
        on date(ds.date) = date(bd.date)
),
sol_data as (
    select date, liquid, native, tvl
    from all_dates
)
select date,
       liquid,
       native,
       tvl
from (
    select * from marinade
        union all
        select * from sol_data
    ) as combined
    order by date desc