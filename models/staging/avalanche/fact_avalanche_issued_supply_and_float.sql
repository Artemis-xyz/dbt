{{ config(materialized="table") }}

with staking_rewards as (
    select
        date,
        cumulative_validator_rewards
    from {{ ref("fact_avalanche_validator_rewards_silver")}}
)
, foundation_balance as (
    select
        date,
        amount_unlocked_native,
        66670000 - amount_unlocked_native as foundation_balance
    from {{ ref("fact_avalanche_cumulative_token_unlocks_silver")}} where allocation_type = 'foundation'
)
, team_balance as (
    select
        date,
        amount_unlocked_native,
        72000000 - amount_unlocked_native as team_balance
    from {{ ref("fact_avalanche_cumulative_token_unlocks_silver")}} where allocation_type = 'team'

)
, burns as (
    select
      date,
      fees_native,
      sum(fees_native) over (order by date) as cumulative_fees_native
    from {{ ref("fact_avalanche_fundamental_data_cte") }}
)

, aggregated AS (
  SELECT
    date,
    720000000 AS max_supply, -- https://build.avax.network/docs/quick-start/avax-token
    360000000 AS initial_supply,
    sum(cumulative_validator_rewards) as staking_rewards_issued,
    max_supply - initial_supply - staking_rewards_issued as uncreated_tokens,
    initial_supply + staking_rewards_issued as total_supply,
    sum(cumulative_fees_native) as cumulative_burned_avax,
    sum(foundation_balance) AS foundation_balances,
    total_supply - cumulative_burned_avax - foundation_balances AS issued_supply,
    sum(team_balance) AS unvested_balances,
    issued_supply - unvested_balances AS circulating_supply_native
  FROM staking_rewards
  left join foundation_balance using(date)
  left join team_balance using(date)
  left join burns using(date)
  GROUP BY date
)

SELECT * FROM aggregated
ORDER BY date