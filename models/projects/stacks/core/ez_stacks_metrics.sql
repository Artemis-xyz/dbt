{{
    config(
        materialized="table",
        snowflake_warehouse="STACKS",
        database="stacks",
        schema="core",
        alias="ez_metrics",
    )
}}

with
    fundamental_data as (
        select
            date,
            max(txns) as txns,
            max(daa) as dau,
            max(fees) as fees,
            max(native_token_fees) as fees_native,
            'stacks' as chain
        from
            (
                {{
                    dbt_utils.union_relations(
                        relations=[
                            ref("fact_stacks_daa_txns"),
                            ref("fact_stacks_native_fees"),
                        ]
                    )
                }}
            )
        group by 1
    ),
    price_data as ({{ get_coingecko_metrics("blockstack") }}),
    defillama_data as ({{ get_defillama_metrics("stacks") }}),
    github_data as ({{ get_github_metrics("stacks") }}),
    rolling_metrics as ({{ get_rolling_active_address_metrics("stacks") }})
select
    fundamental_data.date
    , fundamental_data.chain
    , txns
    , dau
    , wau
    , mau
    , fees_native
    , fees
    , fees / txns as avg_txn_fee

    -- Standardized Metrics
    -- Market Data Metrics
    , price
    , market_cap
    , fdmc
    , token_volume
    , token_turnover_circulating

    -- Usage Metrics
    , tvl

    -- Chain Metrics
    , dau as chain_dau
    , wau as chain_wau
    , mau as chain_mau
    , txns as chain_txns
    
    -- Cashflow Metrics
    , fees as chain_fees
    , fees_native as ecosystem_revenue_native
    , fees as ecosystem_revenue
    , avg_txn_fee as chain_avg_txn_fee
    , ecosystem_revenue as validator_cash_flow
    
    -- Developer Metrics
    , weekly_commits_core_ecosystem
    , weekly_commits_sub_ecosystem
    , weekly_developers_core_ecosystem
    , weekly_developers_sub_ecosystem
from fundamental_data
left join price_data pd on fundamental_data.date = pd.date
left join defillama_data dd on fundamental_data.date = dd.date
left join github_data gd on fundamental_data.date = gd.date
left join rolling_metrics rm on fundamental_data.date = rm.date
where fundamental_data.date < to_date(sysdate())
