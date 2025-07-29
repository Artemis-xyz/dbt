{{
    config(
        materialized="incremental",
        snowflake_warehouse="SONNE_FINANCE",
        database="sonne_finance",
        schema="core",
        alias="ez_metrics",
        incremental_strategy="merge",
        unique_key="date",
        on_schema_change="append_new_columns",
        merge_update_columns=var("backfill_columns", []),
        merge_exclude_columns=["created_on"] if not var("backfill_columns", []) else none,
        full_refresh=false,
        tags=["ez_metrics"]
    )
}}

{% set backfill_date = var("backfill_date", None) %}

with
    sonne_by_chain as (
        {{
            dbt_utils.union_relations(
                relations=[
                    ref("fact_sonne_base_borrows_deposits_gold"),
                    ref("fact_sonne_optimism_borrows_deposits_gold"),
                ],
            )
        }}
    )
    , sonne_metrics as (
        select
            date
            , sum(daily_borrows_usd) as daily_borrows_usd
            , sum(daily_supply_usd) as daily_supply_usd
        from sonne_by_chain
        group by 1
    )
    , price_data as ({{ get_coingecko_metrics("sonne-finance") }})
select
    sonne_metrics.date
    , 'sonne' as app
    , 'DeFi' as category
    , sonne_metrics.daily_borrows_usd
    , sonne_metrics.daily_supply_usd
    -- Standardized metrics
    , sonne_metrics.daily_borrows_usd as lending_loans
    , sonne_metrics.daily_supply_usd as lending_deposits
    , price_data.price
    , price_data.market_cap
    , price_data.fdmc
    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on
from sonne_metrics
left join price_data
    on sonne_metrics.date = price_data.date
where true
{{ ez_metrics_incremental('sonne_metrics.date', backfill_date) }}
and sonne_metrics.date < to_date(sysdate())