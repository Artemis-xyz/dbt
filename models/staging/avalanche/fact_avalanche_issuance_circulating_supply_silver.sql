-- depends_on {{ ref("fact_avalanche_transactions_v2") }}
{{
    config(
        materialized="table",
    )
}}
-- TODO: Track already minted tokens for rewards to validators to calcualte
-- circulating_supply: SEE AVA Labs chat
with
    fundamental_data as ({{ get_fundamental_data_for_chain("avalanche", "v2") }}),
    burns_data as (
        select date, sum(fees_native) over (order by date) as cumulative_burns
        from fundamental_data
    ),
    issuance_data as (
        select date, sum(issuance) as issuance
        from
            (
                select date, validator_rewards as issuance
                from {{ ref("fact_avalanche_validator_rewards_silver") }}
                union all
                select date, unlock_amount_native as issuance
                from {{ ref("fact_avalanche_token_unlocks_silver") }}
            )
        group by date
    ),
    circulating_supply_data as (
        select date, sum(circulating_supply) as circulating_supply
        from
            (
                select date, cumulative_validator_rewards as circulating_supply
                from {{ ref("fact_avalanche_validator_rewards_silver") }}
                union all
                select date, amount_unlocked_native as circulating_supply
                from {{ ref("fact_avalanche_cumulative_token_unlocks_silver") }}
                union all
                select date, - cumulative_burns as circulating_supply
                from burns_data
            )
        group by date
    )
select
    coalesce(t1.date, t2.date) as date,
    'avalanche' as chain,
    coalesce(t1.issuance, 0) as issuance,
    coalesce(t2.circulating_supply, 0) as circulating_supply
from issuance_data t1
full join circulating_supply_data t2 on t1.date = t2.date
where coalesce(t1.date, t2.date) < to_date(sysdate())
