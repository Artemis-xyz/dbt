{{config(materialized="table", snowflake_warehouse='STARGATE')}}

with 
combined_events as (
    {{
        dbt_utils.union_relations(
            relations=[
                ref("dim_stargate_arbitrum_STGholders"),
                ref("dim_stargate_avalanche_STGholders"),
                ref("dim_stargate_bsc_STGholders"),
                ref("dim_stargate_ethereum_STGholders"),
                ref("dim_stargate_optimism_STGholders"),
                ref("dim_stargate_polygon_STGholders"),
            ],
        )
    }}
)
select
    address,
    stg_balance,
    status,
    percentage_of_total_supply,
    staked_balance,
    stake_status as has_staked,
    stake_percentage,
    chain
from combined_events
