{{
    config(
        materialized="table",
        snowflake_warehouse="CARDANO",
        database="cardano",
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
            max(gas_usd) as fees,
            max(gas) as fees_native,
            'cardano' as chain
        from (
            {{
                dbt_utils.union_relations(
                    relations=[
                        ref("fact_cardano_daa"),
                        ref("fact_cardano_txns"),
                        ref("fact_cardano_fees_and_revenue"),
                    ]
                )
            }}
        )
        group by 1
    ),
    price_data as ({{ get_coingecko_metrics("cardano") }}),
    defillama_data as ({{ get_defillama_metrics("cardano") }}),
    github_data as ({{ get_github_metrics("cardano") }})

select
    f.date,
    f.chain,
    txns,
    dau,
    fees_native,
    fees,
    fees / txns as avg_txn_fee,
    dex_volumes,
    -- Standardized Metrics
    -- Market Data
    price,
    market_cap,
    fdmc,
    token_volume,
    -- Chain Metrics
    txns as chain_txns,
    dau as chain_dau,
    avg_txn_fee as chain_avg_txn_fee,
    dex_volumes as chain_spot_volume,
    -- Cash Flow Metrics
    fees as ecosystem_revenue,
    fees as chain_fees,
    fees as gross_protocol_revenue,
    fees_native as gross_protocol_revenue_native,
    -- Crypto Metrics
    tvl,
    -- Developer Metrics
    weekly_commits_core_ecosystem,
    weekly_commits_sub_ecosystem,
    weekly_developers_core_ecosystem,
    weekly_developers_sub_ecosystem,
    token_turnover_circulating,
    token_turnover_fdv
from fundamental_data f
left join price_data on f.date = price_data.date
left join defillama_data on f.date = defillama_data.date
left join github_data on f.date = github_data.date
where f.date < to_date(sysdate())
