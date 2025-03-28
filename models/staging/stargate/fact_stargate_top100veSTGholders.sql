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
from combined_events
