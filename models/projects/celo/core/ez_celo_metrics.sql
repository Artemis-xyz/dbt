--depends_on: {{ ref("agg_celo_stablecoin_metrics") }}
{{
    config(
        materialized="table",
        snowflake_warehouse="CELO",
        database="celo",
        schema="core",
        alias="ez_metrics",
    )
}}

with
    fundamental_data as (
        select
            date, chain, gas_usd as fees, revenue, txns, dau, avg_txn_fee
        from {{ ref("fact_celo_dau_txns_gas_usd_revenue_avg_txn_fee") }}
    ),
    price_data as ({{ get_coingecko_metrics("celo") }}),
    defillama_data as ({{ get_defillama_metrics("celo") }}),
    github_data as ({{ get_github_metrics("celo") }}),
    stablecoin_data as ({{ get_stablecoin_metrics("celo") }})
select
    fundamental_data.date,
    fundamental_data.chain,
    txns,
    dau,
    fees,
    revenue,
    avg_txn_fee,
    price,
    market_cap,
    fdmc,
    tvl,
    dex_volumes,
    weekly_commits_core_ecosystem,
    weekly_commits_sub_ecosystem,
    weekly_developers_core_ecosystem,
    weekly_developers_sub_ecosystem,
    stablecoin_total_supply,
    stablecoin_txns,
    stablecoin_dau,
    stablecoin_mau,
    stablecoin_transfer_volume,
    artemis_stablecoin_txns,
    artemis_stablecoin_dau,
    artemis_stablecoin_mau,
    artemis_stablecoin_transfer_volume,
    p2p_stablecoin_txns,
    p2p_stablecoin_dau,
    p2p_stablecoin_mau,
    p2p_stablecoin_transfer_volume,
from fundamental_data
left join price_data on fundamental_data.date = price_data.date
left join defillama_data on fundamental_data.date = defillama_data.date
left join github_data on fundamental_data.date = github_data.date
left join stablecoin_data on fundamental_data.date = stablecoin_data.date
where fundamental_data.date < to_date(sysdate())