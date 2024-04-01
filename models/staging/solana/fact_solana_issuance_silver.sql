{{ config(materialized="table", snowflake_warehouse="SOLANA_SM") }}
with
    staking_rewards_table as (
        select epoch_earned, sum(coalesce(reward_amount_sol, 0)) as staking
        from solana_flipside.gov.fact_rewards_staking
        group by epoch_earned
        order by epoch_earned desc
    ),
    voting_fees as (
        select epoch_earned, sum(coalesce(reward_amount_sol, 0)) as voting
        from solana_flipside.gov.fact_rewards_voting
        group by epoch_earned
        order by epoch_earned desc
    ),
    issunace_data as (
        select t1.epoch_earned, staking + voting as issuance
        from staking_rewards_table t1
        left join voting_fees t2 on t1.epoch_earned = t2.epoch_earned
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
        group by epoch, start_block
    )
select first_day_in_epoch as date, 'solana' as chain, issuance
from issunace_data t1
left join epoch_to_timestamp_data t2 on t1.epoch_earned = t2.epoch
