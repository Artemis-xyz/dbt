{{ config(materialized="table") }}
with
    metrics_by_chain as (
        {{
            dbt_utils.union_relations(
                relations=[
                    ref("ez_arbitrum_metrics_by_category_v2"),
                    ref("ez_avalanche_metrics_by_category_v2"),
                    ref("ez_base_metrics_by_category_v2"),
                    ref("ez_bsc_metrics_by_category_v2"),
                    ref("ez_ethereum_metrics_by_category_v2"),
                    ref("ez_optimism_metrics_by_category_v2"),
                    ref("ez_polygon_metrics_by_category_v2"),
                    ref("ez_near_metrics_by_category_v2"),
                    ref("ez_sei_metrics_by_category_v2"),
                    ref("ez_tron_metrics_by_category_v2"),
                    ref("ez_mantle_metrics_by_category_v2"),
                    ref("ez_injective_metrics_by_category_v2"),
                    ref("ez_sui_metrics_by_category_v2"),
                    ref("ez_stellar_metrics_by_category_v2"),
                    ref("ez_solana_metrics_by_category_v2"),
                    ref("ez_celo_metrics_by_category_v2")
                ]
            )
        }}
    ), combined_sum as (
        select
            ifnull(category, 'Unlabeled') as category,
            date,
            sum(gas) as gas,
            sum(gas_usd) gas_usd,
            sum(txns) as txns,
            sum(dau) as daa,
            sum(mau) as mau,
            sum(contract_count) as contract_count,
            sum(avg_gas_per_address) as avg_gas_per_address,
            sum(avg_gas_usd_per_address) as avg_gas_usd_per_address,
            sum(rev) as rev,
            sum(rev_usd) as rev_usd,
            sum(real_users) as real_users,
            sum(new_users) as new_users,
            sum(returning_users) as returning_users,
            sum(low_sleep_users) as low_sleep_users,
            sum(high_sleep_users) as high_sleep_users,
            sum(sybil_users) as sybil_users,
            sum(non_sybil_users) as non_sybil_users
        from metrics_by_chain
        group by date, category
    ), category_data as (
        select
            category,
            null as chain,
            date,
            gas,
            gas_usd,
            txns,
            daa,
            mau,
            avg_gas_per_address,
            avg_gas_usd_per_address,
            rev,
            rev_usd,
            contract_count,
            real_users,
            new_users,
            returning_users,
            low_sleep_users,
            high_sleep_users,
            sybil_users,
            non_sybil_users
        from combined_sum
        union
        select
            ifnull(category, 'Unlabeled') as category,
            chain,
            date,
            gas,
            gas_usd,
            txns,
            dau as daa,
            mau,
            avg_gas_per_address,
            avg_gas_usd_per_address,
            rev,
            rev_usd,
            contract_count,
            real_users,
            new_users,
            returning_users,
            low_sleep_users,
            high_sleep_users,
            sybil_users,
            non_sybil_users
        from metrics_by_chain
    )
select
    date,
    case when category = 'Unlabeled' then null else category end as category,
    chain,
    gas as total_gas,
    gas_usd as total_gas_usd,
    txns as transactions,
    daa as dau,
    mau,
    avg_gas_per_address,
    avg_gas_usd_per_address,
    rev,
    rev_usd,
    contract_count,
    real_users,
    returning_users,
    new_users,
    low_sleep_users,
    high_sleep_users,
    sybil_users,
    non_sybil_users
from category_data
where category is not null and txns is not null
