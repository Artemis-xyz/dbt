{{
    config(
        materialized="incremental",
        snowflake_warehouse="SPARK",
        database="spark",
        schema="core",
        alias="ez_metrics",
        incremental_strategy="merge",
        unique_key="date",
        on_schema_change="append_new_columns",
        merge_update_columns=var("backfill_columns", []),
        merge_exclude_columns=["created_on"] if not var("backfill_columns", []) else none,
        full_refresh=var("full_refresh", false),
        tags=["ez_metrics"]
    )
}}

{% set backfill_date = var("backfill_date", None) %}

with
    spark_by_chain as (
        {{
            dbt_utils.union_relations(
                relations=[
                    ref("fact_spark_ethereum_borrows_deposits_gold"),
                    ref("fact_spark_gnosis_borrows_deposits_gold"),
                ],
            )
        }}
    )
    , spark_metrics as (
        select
            date
            , coalesce(sum(daily_borrows_usd), 0) as daily_borrows_usd
            , coalesce(sum(daily_supply_usd), 0) as daily_supply_usd
        from spark_by_chain
        group by 1
    )
    , market_metrics as ( {{ get_coingecko_metrics("spark-2") }} )

select
    spark_metrics.date
    , 'spark' as artemis_id

    -- Standardized Metrics

    -- Market Data
    , market_metrics.price
    , market_metrics.market_cap
    , market_metrics.fdmc
    , market_metrics.token_volume

    -- Usage Data
    , spark_metrics.daily_borrows_usd as lending_loans
    , spark_metrics.daily_supply_usd as lending_deposits

    -- Token Turnover/Other Data
    , market_metrics.token_turnover_circulating
    , market_metrics.token_turnover_fdv

    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on

from spark_metrics
left join market_metrics using (date)
where true
{{ ez_metrics_incremental('spark_metrics.date', backfill_date) }}
and spark_metrics.date < to_date(sysdate())