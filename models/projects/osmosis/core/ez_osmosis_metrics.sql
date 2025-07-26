{{
    config(
        materialized="incremental",
        snowflake_warehouse="OSMOSIS",
        database="osmosis",
        schema="core",
        alias="ez_metrics",
        incremental_strategy="merge",
        unique_key="date",
        on_schema_change="append_new_columns",
        merge_update_columns=var("backfill_columns", []),
        merge_exclude_columns=["created_on"] | reject('in', var("backfill_columns", [])) | list,
        full_refresh=false,
        tags=["ez_metrics"],
    )
}}

{% set backfill_date = var("backfill_date", None) %}

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
    market_data as ({{ get_coingecko_metrics("osmosis") }}),
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
    date_spine.date
    , 'osmosis' as artemis_id
    -- Standardized Metrics

    -- Market Data
    , m.price
    , m.market_cap
    , m.fully_diluted_market_cap
    , m.token_volume

    -- Chain Metrics
    , f.txns as chain_txns
    , f.txns as txns
    , f.dau as chain_dau
    , f.dau as dau
    , f.avg_txn_fee as chain_avg_txn_fee
    , d.dex_volumes as chain_spot_volume -- Osmosis is both a DEX and a chain
    , d.dex_volumes as spot_volume
    , d.tvl

    -- Cash Flow Metrics
    , f.gas_usd as chain_fees
    , f.trading_fees as spot_fees
    , f.fees as fees
    , f.trading_fees as lp_fee_allocation
    , f.gas_usd as validator_fee_allocation
    -- Crypto Metrics

    -- Financial Metrics
    , f.revenue

    -- Developer Metrics
    , g.weekly_commits_core_ecosystem
    , g.weekly_commits_sub_ecosystem
    , g.weekly_developers_core_ecosystem
    , g.weekly_developers_sub_ecosystem
    , p.token_turnover_circulating
    , p.token_turnover_fdv

    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on
from date_spine
left join fundamental_data f using (date)
left join market_data m using (date)
left join defillama_data d using (date)
left join github_data g using (date)
where true
{{ ez_metrics_incremental('date_spine.date', backfill_date) }}
and date_spine.date < to_date(sysdate())
