{{ config(materialized="table") }}

with
    chain_metrics as (
        {{
            dbt_utils.union_relations(
                relations=[
                    ref("ez_arbitrum_metrics"),
                    ref("ez_avalanche_metrics"),
                    ref("ez_base_metrics"),
                    ref("ez_bsc_metrics"),
                    ref("ez_ethereum_metrics"),
                    ref("ez_injective_metrics"),
                    ref("ez_near_metrics"),
                    ref("ez_optimism_metrics"),
                    ref("ez_polygon_metrics"),
                    ref("ez_sei_metrics"),
                    ref("ez_solana_metrics"),
                    ref("ez_stellar_metrics"),
                    ref("ez_sui_metrics"),
                    ref("ez_tron_metrics"),
                    ref("ez_mantle_metrics"),
                    ref("ez_celo_metrics"),
                ]
            )
        }}
    )

select
    date,
    chain,
    txns,
    dau,
    fees_native as gas,
    fees as gas_usd,
    returning_users,
    new_users,
    low_sleep_users,
    high_sleep_users,
    sybil_users,
    non_sybil_users
from chain_metrics
where fees_native is not null
