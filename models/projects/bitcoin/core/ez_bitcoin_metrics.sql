-- depends_on {{ ref("fact_bitcoin_nft_trading_volume") }}
-- depends_on {{ ref("fact_bitcoin_issuance_circulating_supply_silver") }}
{{
    config(
        materialized="table",
        snowflake_warehouse="BITCOIN",
        database="bitcoin",
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
            sum(fees_native) as fees_native,
            sum(fees) as fees,
            sum(fees) / sum(txns) as avg_txn_fee,
            sum(revenue) as revenue,
            'bitcoin' as chain
        from
            (
                {{
                    dbt_utils.union_relations(
                        relations=[
                            ref("fact_bitcoin_daa"),
                            ref("fact_bitcoin_txns"),
                            ref("fact_bitcoin_fees_revenue"),
                        ]
                    )
                }}
            )
        group by 1
    ),
    issuance_data as ({{ get_issuance_metrics("bitcoin") }}),
    price_data as ({{ get_coingecko_metrics("bitcoin") }}),
    defillama_data as ({{ get_defillama_metrics("bitcoin") }}),
    github_data as ({{ get_github_metrics("bitcoin") }}),
    nft_metrics as ({{ get_nft_metrics("bitcoin") }}),
    rolling_metrics as ({{ get_rolling_active_address_metrics("bitcoin") }})

select
    fundamental_data.date,
    fundamental_data.chain,
    txns,
    dau,
    wau,
    mau,
    fees_native,
    fees,
    avg_txn_fee,
    revenue,
    issuance,
    circulating_supply,
    price,
    market_cap,
    fdmc,
    tvl,
    dex_volumes,
    weekly_commits_core_ecosystem,
    weekly_commits_sub_ecosystem,
    weekly_developers_core_ecosystem,
    weekly_developers_sub_ecosystem,
    nft_trading_volume
from fundamental_data
left join issuance_data on fundamental_data.date = issuance_data.date
left join price_data on fundamental_data.date = price_data.date
left join defillama_data on fundamental_data.date = defillama_data.date
left join github_data on fundamental_data.date = github_data.date
left join nft_metrics on fundamental_data.date = nft_metrics.date
left join rolling_metrics on fundamental_data.date = rolling_metrics.date
where fundamental_data.date < to_date(sysdate())
