{{
    config(
        materialized="table",
        database="avalanche",
        schema="core",
        alias="fact_staking_metrics_by_type",
        snowflake_warehouse="AVALANCHE",
    )
}}
select
    date,
    validator_stake_native,
    validator_stake_usd,
    delegator_stake_native,
    delegator_stake_usd,
    total_staked_native,
    total_staked_usd
from {{ ref("fact_avalanche_amount_staked_silver") }}
