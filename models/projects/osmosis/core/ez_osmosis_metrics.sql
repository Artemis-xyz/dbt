{{
    config(
        materialized="table",
        snowflake_warehouse="OSMOSIS",
        database="osmosis",
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
            sum(gas_usd) as gas_usd,
            sum(trading_fees) as trading_fees,
            sum(fees) as fees,
            sum(revenue) as revenue,
            'osmosis' as chain
        from
            (
                {{
                    dbt_utils.union_relations(
                        relations=[
                            ref("fact_osmosis_daa_txns"),
                            ref("fact_osmosis_gas_gas_usd_fees_revenue"),
                        ]
                    )
                }}
            )
        group by 1
    ),
    price_data as ({{ get_coingecko_metrics("osmosis") }}),
    defillama_data as ({{ get_defillama_metrics("osmosis") }}),
    github_data as ({{ get_github_metrics("osmosis") }})
select
    fundamental_data.date,
    fundamental_data.chain,
    txns,
    dau,
    gas_usd,
    trading_fees,
    fees,
    revenue,
    price,
    market_cap,
    tvl,
    dex_volumes,
    weekly_commits_core_ecosystem,
    weekly_commits_sub_ecosystem,
    weekly_developers_core_ecosystem,
    weekly_developers_sub_ecosystem
from fundamental_data
left join price_data on fundamental_data.date = price_data.date
left join defillama_data on fundamental_data.date = defillama_data.date
left join github_data on fundamental_data.date = github_data.date
where fundamental_data.date < to_date(sysdate())
