{{
    config(
        materialized="table",
        snowflake_warehouse="STARGATE",
        database="stargate",
        schema="core",
        alias="ez_metrics_top100STGholders",
    )
}}

with
top100STGholders as (
    select
        address,
        stg_balance,
        status,
        percentage_of_total_supply,
        staked_balance,
        has_staked,
        stake_percentage,
        chain
    from {{ref("fact_stargate_top100STGholders")}}
)

select
    address,
    stg_balance,
    status,
    percentage_of_total_supply,
    staked_balance,
    has_staked,
    stake_percentage,
    chain
from top100STGholders