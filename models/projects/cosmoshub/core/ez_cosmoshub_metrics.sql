{{
    config(
        materialized="incremental",
        snowflake_warehouse="COSMOSHUB",
        database="cosmoshub",
        schema="core",
        alias="ez_metrics",
        incremental_strategy="merge",
        unique_key="date",
        on_schema_change="append_new_columns",
        merge_update_columns=var("backfill_columns", []),
        merge_exclude_columns=["created_on"] if not var("backfill_columns", []) else none,
        full_refresh=false,
        tags=["ez_metrics"],
    )
}}

{% set backfill_date = var("backfill_date", None) %}

with
    fundamental_data as (
        select
            date,
            sum(txns) as txns,
            sum(daa) as dau,
            sum(gas_usd) as fees,
            sum(revenue) as revenue,
            sum(wau) as wau,
            sum(mau) as mau,
            'cosmoshub' as chain
        from
            (
                {{
                    dbt_utils.union_relations(
                        relations=[
                            ref("fact_cosmoshub_daa"),
                            ref("fact_cosmoshub_txns"),
                            ref("fact_cosmoshub_fees_and_revenue"),
                            ref("fact_cosmoshub_rolling_active_addresses"),
                        ]
                    )
                }}
            )
        group by 1
    ),
    market_data as ({{ get_coingecko_metrics("cosmos") }}),
    defillama_data as ({{ get_defillama_metrics("cosmos") }}),
    github_data as ({{ get_github_metrics("cosmos") }})
select
    f.date
    , 'cosmoshub' as artemis_id
    -- Standardized Metrics
    -- Market Data
    , market_data.price
    , market_data.market_cap
    , market_data.fdmc
    , market_data.token_volume
    -- Chain Metrics
    , f.txns as chain_txns
    , f.txns
    , f.daa as chain_dau
    , f.daa
    , f.wau as chain_wau
    , f.mau as chain_mau
    , f.avg_txn_fee as chain_avg_txn_fee

    -- Fee Metrics
    , f.fees as chain_fees
    , f.fees as fees
    , f.revenue as treasury_fee_allocation

    -- Financial Metrics
    , f.revenue

    -- Crypto Metrics
    , defillama_data.tvl
    -- Developer Metrics
    , github_data.weekly_commits_core_ecosystem
    , github_data.weekly_commits_sub_ecosystem
    , github_data.weekly_developers_core_ecosystem
    , github_data.weekly_developers_sub_ecosystem
    
    -- Other Metrics
    , market_data.token_turnover_circulating
    , market_data.token_turnover_fdv

    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on
from fundamental_data f
left join market_data on f.date = market_data.date
left join defillama_data on f.date = defillama_data.date
left join github_data on f.date = github_data.date
where true
{{ ez_metrics_incremental("f.date", backfill_date) }}
and f.date < to_date(sysdate())
