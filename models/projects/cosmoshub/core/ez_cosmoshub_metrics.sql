{{
    config(
        materialized="table",
        snowflake_warehouse="COSMOSHUB",
        database="cosmoshub",
        schema="core",
        alias="ez_metrics",
    )
}}

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
    price_data as ({{ get_coingecko_metrics("cosmos") }}),
    defillama_data as ({{ get_defillama_metrics("cosmos") }}),
    github_data as ({{ get_github_metrics("cosmos") }})
select
    f.date
    , f.chain
    , txns
    , dau
    , wau
    , mau
    , fees
    , fees / txns as avg_txn_fee
    , revenue
    -- Standardized Metrics
    -- Market Data
    , price
    , market_cap
    , fdmc
    , token_volume
    -- Chain Metrics
    , txns as chain_txns
    , dau as chain_dau
    , wau as chain_wau
    , mau as chain_mau
    , avg_txn_fee as chain_avg_txn_fee
    -- Cash Flow Metrics
    , fees as chain_fees
    , fees as ecosystem_revenue
    , revenue as treasury_fee_allocation
    -- Crypto Metrics
    , tvl
    -- Developer Metrics
    , weekly_commits_core_ecosystem
    , weekly_commits_sub_ecosystem
    , weekly_developers_core_ecosystem
    , weekly_developers_sub_ecosystem
    , token_turnover_circulating
    , token_turnover_fdv
from fundamental_data f
left join price_data on f.date = price_data.date
left join defillama_data on f.date = defillama_data.date
left join github_data on f.date = github_data.date
where f.date < to_date(sysdate())
