{{ config(materialized="table") }}
with
    metrics_by_chain as (
        {{
            dbt_utils.union_relations(
                relations=[
                    ref("ez_arbitrum_metrics_by_application"),
                    ref("ez_avalanche_metrics_by_application"),
                    ref("ez_base_metrics_by_application"),
                    ref("ez_bsc_metrics_by_application"),
                    ref("ez_ethereum_metrics_by_application"),
                    ref("ez_near_metrics_by_application"),
                    ref("ez_optimism_metrics_by_application"),
                    ref("ez_polygon_metrics_by_application"),
                    ref("ez_solana_metrics_by_application"),
                    ref("ez_tron_metrics_by_application"),
                ]
            )
        }}
    ),
    combined_sum as (
        select
            app,
            max(friendly_name) as friendly_name,
            date,
            max(category) as category,
            sum(gas) as gas,
            sum(gas_usd) gas_usd,
            sum(txns) as txns,
            sum(dau) as daa,
            sum(returning_users) as returning_users,
            sum(new_users) as new_users,
            sum(low_sleep_users) as low_sleep_users,
            sum(high_sleep_users) as high_sleep_users,
            sum(sybil_users) as sybil_users,
            sum(non_sybil_users) as non_sybil_users
        from metrics_by_chain
        group by date, app
    )
select
    date,
    app,
    friendly_name,
    null as chain,
    category,
    gas,
    gas_usd,
    txns,
    daa,
    returning_users,
    new_users,
    low_sleep_users,
    high_sleep_users,
    sybil_users,
    non_sybil_users
from combined_sum
union
select
    date,
    app,
    friendly_name,
    chain,
    category,
    gas,
    gas_usd,
    txns,
    dau as daa,
    returning_users,
    new_users,
    low_sleep_users,
    high_sleep_users,
    sybil_users,
    non_sybil_users
from metrics_by_chain
