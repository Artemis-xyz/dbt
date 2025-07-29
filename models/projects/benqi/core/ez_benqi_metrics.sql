{{
    config(
        materialized="incremental",
        snowflake_warehouse="BENQI_FINANCE",
        database="benqi_finance",
        schema="core",
        alias="ez_metrics",
        incremental_strategy="merge",
        unique_key="date",
        on_schema_change="append_new_columns",
        merge_update_columns=var("backfill_columns", []),
        merge_exclude_columns=["created_on"] if not var("backfill_columns", []) else none,
        full_refresh=false,
        tags=["ez_metrics"],
    )
}}

{% set backfill_date = var("backfill_date", None) %}

with
    benqi_by_chain as (
        {{
            dbt_utils.union_relations(
                relations=[
                    ref("fact_benqi_avalanche_borrows_deposits_gold"),
                ],
            )
        }}
    )
    , benqi_metrics as (
        select
            date
            , sum(daily_borrows_usd) as daily_borrows_usd
            , sum(daily_supply_usd) as daily_supply_usd
        from benqi_by_chain
        group by 1
    )
    , price_data as ({{ get_coingecko_metrics("benqi") }})

select
    benqi_metrics.date
    , 'benqi' as app
    , 'DeFi' as category
    -- Standardized metrics
    , benqi_metrics.daily_borrows_usd as lending_loans
    , benqi_metrics.daily_supply_usd as lending_deposits
    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on
from benqi_metrics
left join price_data
    on benqi_metrics.date = price_data.date
where true
{{ ez_metrics_incremental('benqi_metrics.date', backfill_date) }}
and benqi_metrics.date < to_date(sysdate())