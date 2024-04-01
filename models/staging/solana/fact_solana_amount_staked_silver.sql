{{ config(materialized="table", snowflake_warehouse="SOLANA_SM") }}
with
    sol_staked_by_epoch as (
        select
            epoch,
            count(distinct vote_pubkey) as validators,
            sum(
                case when deactivation_epoch > epoch then active_stake else 0 end
            ) as total_staked
        from solana_flipside.gov.fact_stake_accounts
        where epoch >= 450
        group by epoch
        order by epoch
    ),
    max_block_on_sol as (
        select max(block_id) as max_block from solana_flipside.core.fact_blocks
    ),
    epoch_to_timestamp_data as (
        select
            epoch,
            start_block,
            min(block_id) as first_block_in_epoch,
            min(block_timestamp) as first_timestamp_in_epoch,
            to_date(min(block_timestamp)) as first_day_in_epoch
        from solana_flipside.gov.dim_epoch t1
        left join
            solana_flipside.core.fact_blocks t2
            on t2.block_id >= t1.start_block
            and t2.block_id <= end_block
        right join max_block_on_sol on max_block >= t2.block_id
        where epoch >= 450
        group by epoch, start_block
    ),
    block_data as (
        select t1.epoch, t2.first_day_in_epoch as date, t1.validators, t1.total_staked
        from sol_staked_by_epoch t1
        left join epoch_to_timestamp_data t2 on t1.epoch = t2.epoch
    ),
    date_series as (
        select min(date) as date
        from block_data
        union all
        select dateadd(day, 1, date)
        from date_series
        where date < to_date(sysdate())
    ),
    full_data as (
        select ds.date, t.validators, t.total_staked
        from date_series ds
        left join block_data t on ds.date = t.date
    ),
    forward_filled as (
        select
            date,
            last_value(validators ignore nulls) over (
                order by date rows between unbounded preceding and current row
            ) as validators,
            last_value(total_staked ignore nulls) over (
                order by date rows between unbounded preceding and current row
            ) as total_staked
        from full_data
    ),
    prices as ({{ get_coingecko_price_with_latest("solana") }})

select
    forward_filled.date,
    'solana' as chain,
    validators as total_validators,
    total_staked as total_staked_native,
    total_staked * coalesce(price, 0) as total_staked_usd
from forward_filled
left join prices on forward_filled.date = prices.date
where forward_filled.date < to_date(sysdate())
order by date
