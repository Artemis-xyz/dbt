{{ config(snowflake_warehouse="ETHEREUM_XS", materialized="table") }}

with daily_block_rewards as (
    select
    date_trunc('day', block_timestamp) AS date,
    sum(case
    -- initial supply from table balances_ethereum.genesis_balances plus inital block reward
    when block_number = 1 then 72009990.49947989 + 5
    -- block reward till byazntium upgrade
    when block_number > 1 and block_number < 4370000 then 5
    -- byzantium upgrad which reduced 5 block rewards down to 3
    when block_number >= 4370000 and block_number < 7280000 then 3
    -- pre-merge reward
    when block_number >= 7280000 and block_number < 15537393 then 2
    else 0 end) as block_rewards
    from ethereum_flipside.core.fact_blocks
    group by 1
),
validators as (
    select date, round(total_staked_native/32, 0) as validators from {{ ref("fact_ethereum_amount_staked_silver") }}
),
issuance as (
    select
        d.date,
    case
    -- block rewards + uncle rewards
    when d.date < '2020-12-01' then block_rewards + 1243
    -- staking + uncle rewards
    when d.date >= '2020-12-01' and d.date < '2022-09-15' then (940.8659 / 365 * sqrt(validators)) + 1243 + block_rewards
    -- staking rewards only
    else block_rewards + (940.8659 / 365 * sqrt(validators))
    end as daily_issuance
    from daily_block_rewards d
    left join validators c on d.date = c.date
)
select
    date,
    daily_issuance as block_rewards_native
from issuance
where date < to_date(sysdate())