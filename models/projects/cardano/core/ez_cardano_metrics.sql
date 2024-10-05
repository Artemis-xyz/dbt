--depends_on: {{ ref("fact_cardano_nft_trading_volume") }}
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
        from
            (
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
    github_data as ({{ get_github_metrics("cardano") }}),
    nft_metrics as ({{ get_nft_metrics("cardano") }})
select
    fundamental_data.date,
    fundamental_data.chain,
    txns,
    dau,
    fees_native,
    fees,
    fees / txns as avg_txn_fee,
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
left join price_data on fundamental_data.date = price_data.date
left join defillama_data on fundamental_data.date = defillama_data.date
left join github_data on fundamental_data.date = github_data.date
left join nft_metrics on fundamental_data.date = nft_metrics.date
where fundamental_data.date < to_date(sysdate())
