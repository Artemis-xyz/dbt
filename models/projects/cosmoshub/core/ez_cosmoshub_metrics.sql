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
            'cosmoshub' as chain
        from
            (
                {{
                    dbt_utils.union_relations(
                        relations=[
                            ref("fact_cosmoshub_daa_gold"),
                            ref("fact_cosmoshub_txns_gold"),
                            ref("fact_cosmoshub_fees_and_revenue_gold"),
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
    fundamental_data.date,
    fundamental_data.chain,
    txns,
    dau,
    fees,
    revenue,
    price,
    market_cap,
    fdmc,
    tvl,
    weekly_commits_core_ecosystem,
    weekly_commits_sub_ecosystem,
    weekly_developers_core_ecosystem,
    weekly_developers_sub_ecosystem
from fundamental_data
left join price_data on fundamental_data.date = price_data.date
left join defillama_data on fundamental_data.date = defillama_data.date
left join github_data on fundamental_data.date = github_data.date
where fundamental_data.date < to_date(sysdate())
