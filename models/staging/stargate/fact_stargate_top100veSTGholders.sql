{{config(materialized="table", snowflake_warehouse='STARGATE')}}

with 
combined_events as (
    {{
        dbt_utils.union_relations(
            relations=[
                ref("dim_stargate_arbitrum_veSTGholders"),
                ref("dim_stargate_avalanche_veSTGholders"),
                ref("dim_stargate_bsc_veSTGholders"),
                ref("dim_stargate_ethereum_veSTGholders"),
                ref("dim_stargate_optimism_veSTGholders"),
                ref("dim_stargate_polygon_veSTGholders"),
            ],
        )
    }}
)

, total_supply_veSTG as (
    select sum(veSTG_balance) as total_supply
    from combined_events
)

, weighted_stake_addresses as (
    select
        ce.from_address,
        ce.stg_balance,
        ce.veSTG_balance,
        (ce.veSTG_balance / ts.total_supply) * 100 as percentage_of_total_supply,
        ce.remaining_days,
        ce.remaining_staking_period,
        ce.number_of_votes_cast,
        ce.last_voted_timestamp,
        ce.last_change_timestamp,
        ce.last_action_type,
        ce.num_days_staked,
        ce.chain,
        ce.num_days_staked * (percentage_of_total_supply / 100.0) as weighted_stake_days
    from combined_events ce
    join total_supply_veSTG ts
)

, total_stake_days as (
    select sum(weighted_stake_days) as total_stake_days
    from weighted_stake_addresses
)

, stake_day_ratio as (
    select
        wsa.*,
        tsd.total_stake_days,
        wsa.weighted_stake_days / nullif(tsd.total_stake_days, 0) as stake_day_ratio
    from weighted_stake_addresses wsa
    cross join total_stake_days tsd
)

, veSTG_fees as (
    select sum(fees) * 1.0 / 6.0 as total_fees
    from {{ ref("fact_stargate_v2_transfers") }}
)

select
    sdr.from_address,
    sdr.stg_balance,
    sdr.veSTG_balance,
    sdr.percentage_of_total_supply,
    sdr.remaining_days,
    sdr.remaining_staking_period,
    sdr.number_of_votes_cast,
    sdr.last_voted_timestamp,
    sdr.last_change_timestamp,
    sdr.last_action_type,
    sdr.num_days_staked,
    sdr.weighted_stake_days,
    v.total_fees * sdr.stake_day_ratio as fees_received,
    sdr.chain
from stake_day_ratio sdr
cross join veSTG_fees v
order by veSTG_balance desc