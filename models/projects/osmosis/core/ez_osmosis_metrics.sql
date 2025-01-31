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
    date_spine as (
        select date from {{ ref("dim_date_spine") }}
        where date between '2021-06-23' and to_date(sysdate())
    ),
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
    defillama_data as (
        with raw as ({{ get_defillama_metrics("osmosis") }})
        , sparse_data as (
            select
                date_spine.date,
                raw.dex_volumes,
                raw.tvl
            from date_spine
            left join raw using (date)
        )
        , filled_data as (
            select
                date,
                COALESCE(sparse_data.dex_volumes, 
                    LAST_VALUE(sparse_data.dex_volumes IGNORE NULLS) OVER (ORDER BY date ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
                    ) as dex_volumes,
                COALESCE(sparse_data.tvl, 
                    LAST_VALUE(sparse_data.tvl IGNORE NULLS) OVER (ORDER BY date ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
                    ) as tvl
            from sparse_data
        )
        select
            date,
            dex_volumes,
            tvl
        from filled_data
    )
    , github_data as ({{ get_github_metrics("osmosis") }})
select
    date_spine.date,
    fundamental_data.chain,
    txns,
    dau,
    gas_usd,
    trading_fees,
    fees,
    fees / txns as avg_txn_fee,
    revenue,
    price,
    market_cap,
    tvl,
    dex_volumes,
    weekly_commits_core_ecosystem,
    weekly_commits_sub_ecosystem,
    weekly_developers_core_ecosystem,
    weekly_developers_sub_ecosystem
from date_spine
left join fundamental_data using (date)
left join price_data using (date)
left join defillama_data using (date)
left join github_data using (date)
where date_spine.date < to_date(sysdate())
