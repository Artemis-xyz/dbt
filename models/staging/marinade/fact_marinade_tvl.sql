{{ config(
    materialized="table",
    snowflake_warehouse="MARINADE"
) }}

with
    sol_staked_by_epoch as (
        select
            s.epoch,
            sum(
                case 
                    when s.authorized_staker = '4bZ6o3eUUNXhKuqjdCnCoPAoLgWiuLYixKaxoa8PpiKk' then s.active_stake
                    else 0
                end
            ) as liquid,
            sum(
                case 
                    when s.authorized_staker = 'stWirqFCf2Uts1JBL1Jsd3r6VBWhgnpdPxCTe1MFjrq' then s.active_stake
                    else 0
                end
            ) as native,
            sum(
                case 
                    when s.authorized_staker in ('4bZ6o3eUUNXhKuqjdCnCoPAoLgWiuLYixKaxoa8PpiKk', 'stWirqFCf2Uts1JBL1Jsd3r6VBWhgnpdPxCTe1MFjrq') then s.active_stake
                    else 0
                end
            ) as tvl
        from 
            solana_flipside.gov.fact_stake_accounts s
        where 
            s.authorized_staker in ('4bZ6o3eUUNXhKuqjdCnCoPAoLgWiuLYixKaxoa8PpiKk', 'stWirqFCf2Uts1JBL1Jsd3r6VBWhgnpdPxCTe1MFjrq')
        group by 
            s.epoch
    ),
    epoch_to_timestamp_data as (
        select
            e.epoch,
            min(fb.block_timestamp) as epoch_start_timestamp
        from 
            solana_flipside.gov.dim_epoch e
        left join 
            solana_flipside.core.fact_blocks fb on fb.block_id between e.start_block and e.end_block
        where 
            e.epoch >= 330
        group by 
            e.epoch
    ),
    block_data as (
        select 
            se.epoch, 
            etd.epoch_start_timestamp as date, 
            se.liquid, 
            se.native, 
            se.tvl
        from 
            sol_staked_by_epoch se
        left join 
            epoch_to_timestamp_data etd on se.epoch = etd.epoch
        where date(etd.epoch_start_timestamp) >= '2023-09-03'
    ),
    date_spine as (
        select
            date
        from
            pc_dbt_db.prod.dim_date_spine
        where date between (select min(date) from block_data) and to_date(sysdate())
    ),
    all_dates as (
        select
            ds.date,
            coalesce(last_value(bd.liquid IGNORE NULLS) over (
                order by ds.date rows between unbounded preceding and current row
            ), 0) AS liquid,
            coalesce(last_value(bd.native IGNORE NULLS) over (
                order by ds.date rows between unbounded preceding and current row
            ), 0) AS native,
            coalesce(last_value(bd.tvl IGNORE NULLS) over (
                order by ds.date rows between unbounded preceding and current row
            ), 0) AS tvl
        from
            date_spine ds
        left join 
            block_data bd on DATE(ds.date) = DATE(bd.date)
        order by
            ds.date
)
select 
    date,
    liquid,
    native,
    tvl
from 
    all_dates
order by 
    date desc

