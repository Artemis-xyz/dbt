{{
    config(
        materialized="table",
        snowflake_warehouse="STARGATE",
        database="stargate",
        schema="core",
        alias="ez_metrics_top100veSTGholders",
    )
}}

with
top100veSTGholders as (
    select
        from_address,
        stg_balance,
        veSTG_balance,
        remaining_days,
        remaining_staking_period,
        percentage_of_total_supply,
        number_of_votes_cast,
        last_voted_timestamp,
        last_change_timestamp,
        last_action_type,
        fees_received,
        chain
    from {{ref("fact_stargate_top100veSTGholders")}}
)

select
    from_address,
    stg_balance,
    veSTG_balance,
    remaining_days,
    remaining_staking_period,
    percentage_of_total_supply,
    number_of_votes_cast,
    last_voted_timestamp,
    last_change_timestamp,
    last_action_type,
    fees_received,
    chain
from top100veSTGholders