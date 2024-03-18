{{ config(materialized="table") }}
select
    trunc(block_timestamp, 'day') as date,
    sum(total_reward) total_reward,
    sum(block_reward) block_reward,
    sum(fees) fees,
    'bitcoin' as chain
from bitcoin_flipside.gov.ez_miner_rewards
group by 1
order by 1 asc
