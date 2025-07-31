{{
    config(
        materialized="table",
        snowflake_warehouse="FUSE",
        database="fuse",
        schema="core",
        alias="ez_metrics_by_chain",
    )
}}

with
    fundamental_data as (
        select
            date
            , txns
            , daa as dau
            , gas_usd as fees
            , gas as fees_native
        from {{ ref("fact_fuse_daa_txns_gas_gas_usd") }}
    )
    , github_data as ({{ get_github_metrics("fuse") }})
    , defillama_data as ({{ get_defillama_metrics("fuse") }})
select
    fundamental_data.date
    , 'fuse' as artemis_id
    , 'fuse' as chain
    
    -- Standardized Metrics
    
    -- Usage Metrics
    , fundamental_data.dau as chain_dau
    , fundamental_data.dau as dau
    , fundamental_data.txns as chain_txns
    , fundamental_data.txns as txns
    , fundamental_data.avg_txn_fee as chain_avg_txn_fee
    , fundamental_data.dex_volumes as chain_spot_volume
    , defillama_data.tvl as chain_tvl
    , defillama_data.tvl as tvl

    -- Cash Flow Metrics
    , fundamental_data.fees_native as fees_native
    , fundamental_data.fees as chain_fees
    , fundamental_data.fees as fees
    
    -- Developer Metrics
    , github_data.weekly_commits_core_ecosystem
    , github_data.weekly_commits_sub_ecosystem
    , github_data.weekly_developers_core_ecosystem
    , github_data.weekly_developers_sub_ecosystem

from fundamental_data
left join github_data using (date)
left join defillama_data using (date)
where fundamental_data.date < to_date(sysdate())