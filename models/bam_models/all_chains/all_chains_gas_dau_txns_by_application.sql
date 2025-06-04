{{ config(materialized="table") }}

with
    metrics_by_chain as (
        {{
            dbt_utils.union_relations(
                relations=[
                    ref("ez_arbitrum_metrics_by_application_v2"),
                    ref("ez_avalanche_metrics_by_application_v2"),
                    ref("ez_base_metrics_by_application_v2"),
                    ref("ez_bsc_metrics_by_application_v2"),
                    ref("ez_ethereum_metrics_by_application_v2"),
                    ref("ez_optimism_metrics_by_application_v2"),
                    ref("ez_polygon_metrics_by_application_v2"),
                    ref("ez_near_metrics_by_application_v2"),
                    ref("ez_sei_metrics_by_application_v2"),
                    ref("ez_tron_metrics_by_application_v2"),
                    ref("ez_mantle_metrics_by_application_v2"),
                    ref("ez_injective_metrics_by_application_v2"),
                    ref("ez_sui_metrics_by_application_v2"),
                    ref("ez_stellar_metrics_by_application_v2"),
                    ref("ez_solana_metrics_by_application_v2"),
                    ref("ez_celo_metrics_by_application_v2")
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
            max(sub_category) as sub_category,
            sum(gas) as gas,
            sum(gas_usd) gas_usd,
            sum(txns) as txns,
            sum(dau) as daa,
            sum(contract_count) as contract_count,
            sum(real_users) as real_users,
            sum(returning_users) as returning_users,
            sum(new_users) as new_users,
            sum(low_sleep_users) as low_sleep_users,
            sum(high_sleep_users) as high_sleep_users,
            sum(sybil_users) as sybil_users,
            sum(non_sybil_users) as non_sybil_users
        from metrics_by_chain
        group by date, app
    ), app_data as (
        select
            date,
            app,
            friendly_name,
            null as chain,
            category,
            sub_category,
            gas,
            gas_usd,
            txns,
            daa,
            contract_count,
            real_users,
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
            sub_category,
            gas,
            gas_usd,
            txns,
            dau as daa,
            contract_count,
            real_users,
            returning_users,
            new_users,
            low_sleep_users,
            high_sleep_users,
            sybil_users,
            non_sybil_users
        from metrics_by_chain
    ), namespace_data as (
        select
            date,
            app as namespace,
            friendly_name,
            category,
            sub_category,
            chain,
            txns as transactions,
            daa as dau,
            contract_count,
            real_users,
            gas as total_gas,
            gas_usd as total_gas_usd,
            returning_users,
            new_users,
            low_sleep_users,
            high_sleep_users,
            sybil_users,
            non_sybil_users
        from app_data
        where app is not null and txns is not null
    ),
    namespace_coingecko as (
        select
            artemis_application_id as namespace,
            token_image_thumb as image_thumbnail,
            token_image_small as image_small,
            cg.coingecko_token_id as coingecko_id
        from {{ ref("dim_all_apps_gold") }} pn
        left join
            {{ ref("dim_coingecko_tokens") }} cg
            on pn.coingecko_id = cg.coingecko_token_id
    )
select
    date,
    ns.namespace,
    friendly_name,
    category,
    sub_category,
    chain,
    image_thumbnail,
    image_small,
    coingecko_id,
    total_gas,
    total_gas_usd,
    transactions,
    dau,
    contract_count,
    real_users,
    returning_users,
    new_users,
    high_sleep_users,
    low_sleep_users,
    sybil_users,
    non_sybil_users
from namespace_data ns
left join namespace_coingecko cg on ns.namespace = cg.namespace
