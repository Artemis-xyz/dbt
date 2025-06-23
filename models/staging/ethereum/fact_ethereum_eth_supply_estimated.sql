{{
    config(
        materialized="table",
        snowflake_warehouse="ETHEREUM",
    )
}}
with base_rewards as (
    select
        block_number,
        CASE WHEN block_timestamp < '2015-07-30' THEN date('2015-07-29')
        ELSE block_timestamp::date END as date,
        case
            when block_number < 4370000 then 5
            when block_number < 7280000 then 3
            when block_number < 15537393 then 2
            else 0
        end as base_reward,
        array_size(uncle_blocks) as num_uncles
    from ethereum_flipside.core.fact_blocks
)
, validators_per_slot as (
    SELECT
        b.slot_timestamp,
        count(distinct index) as n_validators
    FROM ethereum_flipside.beacon_chain.fact_validators v
    LEFT JOIN ethereum_flipside.beacon_chain.fact_blocks b USING(slot_number)
    WHERE VALIDATOR_STATUS IN (
        'active_ongoing',
        'active_exiting',
        'active_slashed'
    )
    AND b.slot_timestamp >= '2022-09-15'
    GROUP BY 1
)
, staking_rewards as (
    SELECT
        slot_timestamp::date as date,
        avg(n_validators) as n_validators_daily_avg,
        (940.8659 / 365 * sqrt(avg(n_validators))) as daily_staking_rewards
    FROM validators_per_slot
    GROUP BY 1
)
, pow_components as (
    select
        date,
        sum(base_reward) as total_base_reward,
        count(*) as block_count,
        sum(num_uncles * base_reward / 32.0) as total_inclusion_reward,
        sum(
            case 
                when block_number < 15537393 then num_uncles * (base_reward / 1)
                else 0
            end
        ) as estimated_uncle_miner_reward,
        sum(num_uncles) as daily_uncles,
        sum(base_reward) +
        sum(num_uncles * base_reward / 32.0) +
        sum(case when block_number < 15537393 then num_uncles * (base_reward * 0.9) else 0 end) as total_issuance
    from base_rewards
    group by date
    order by date
)
, pos_components as (
    SELECT
        date
        , daily_staking_rewards as daily_staking_rewards
        , native_token_burn as burns_native
    FROM agg_daily_ethereum_revenue
    left join staking_rewards using(date)

)
, eth_supply_components as (
    SELECT
        date,
        total_base_reward,
        block_count,
        total_inclusion_reward,
        estimated_uncle_miner_reward,
        daily_uncles,
        total_issuance,
        daily_staking_rewards,
        burns_native,
        coalesce(total_issuance,0) + coalesce(daily_staking_rewards,0) - coalesce(burns_native,0) as net_supply_change
    FROM pow_components
    LEFT JOIN pos_components USING(date)
)
, foundation_eth_balances as (
    SELECT date, sum(balance_native) as foundation_balance FROM fact_ethereum_foundation_balance
    WHERE contract_address ilike '%eip%'
    GROUP BY 1
)
SELECT
    date
    , total_base_reward
    , block_count
    , total_inclusion_reward
    , estimated_uncle_miner_reward
    , daily_uncles
    , burns_native
    , 72009990.49947989 as initial_supply
    , coalesce(total_issuance,0) + coalesce(daily_staking_rewards,0) as issuance
    , coalesce(burns_native,0) as burns
    , sum(net_supply_change) OVER (ORDER BY DATE ASC) + 72009990.49947989 AS total_supply
    , foundation_balance
    , sum(net_supply_change) OVER (ORDER BY DATE ASC) + 72009990.49947989 - foundation_balance as issued_supply
    , sum(net_supply_change) OVER (ORDER BY DATE ASC) + 72009990.49947989 - foundation_balance as circulating_supply
FROM eth_supply_components
LEFT JOIN foundation_eth_balances USING(date)