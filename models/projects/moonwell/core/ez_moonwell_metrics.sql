{{
    config(
        materialized="incremental",
        snowflake_warehouse="MOONWELL",
        database="moonwell",
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
    moonwell_by_chain as (
        {{
            dbt_utils.union_relations(
                relations=[
                    ref("fact_moonwell_base_borrows_deposits_gold"),
                ],
            )
        }}
    )
    , moonwell_metrics as (
        select
            date
            , sum(daily_borrows_usd) as daily_borrows_usd
            , sum(daily_supply_usd) as daily_supply_usd
        from moonwell_by_chain
        group by 1
    )
    , market_data as ({{ get_coingecko_metrics("moonwell-artemis") }})

select
    moonwell_metrics.date
    , 'moonwell' as artemis_id

    -- Standardized Metrics
    , market_data.price
    , market_data.market_cap
    , market_data.fdmc
    , market_data.token_volume

    -- Usage Metrics
    , moonwell_metrics.daily_borrows_usd as lending_loans
    , moonwell_metrics.daily_supply_usd as lending_deposits

    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on
from moonwell_metrics
left join market_data
    on moonwell_metrics.date = market_data.date
where true
{{ ez_metrics_incremental('moonwell_metrics.date', backfill_date) }}
and moonwell_metrics.date < to_date(sysdate())