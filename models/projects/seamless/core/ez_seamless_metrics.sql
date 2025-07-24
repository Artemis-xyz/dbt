{{
    config(
        materialized="incremental",
        snowflake_warehouse="SEAMLESSPROTOCOL",
        database="seamlessprotocol",
        schema="core",
        alias="ez_metrics",
        incremental_strategy="merge",
        unique_key="date",
        on_schema_change="append_new_columns",
        merge_update_columns=var("backfill_columns", []),
        merge_exclude_columns=["created_on"] | reject('in', var("backfill_columns", [])) | list,
        full_refresh=false,
        tags=["ez_metrics"]
    )
}}

{% set backfill_date = var("backfill_date", None) %}

with
    seamless_by_chain as (
        {{
            dbt_utils.union_relations(
                relations=[
                    ref("fact_seamless_protocol_base_borrows_deposits_gold"),
                ],
            )
        }}
    )
    , seamless_metrics as (
        select
            date
            , sum(daily_borrows_usd) as daily_borrows_usd
            , sum(daily_supply_usd) as daily_supply_usd
        from seamless_by_chain
        group by 1
    )

    , price_data as ({{ get_coingecko_metrics("seamless-protocol") }})

select
    seamless_metrics.date
    , 'seamless' as app
    , 'DeFi' as category
    , seamless_metrics.daily_borrows_usd
    , seamless_metrics.daily_supply_usd
    -- Standardized metrics
    , seamless_metrics.daily_borrows_usd as lending_loans
    , seamless_metrics.daily_supply_usd as lending_deposits
    , price_data.price
    , price_data.market_cap
    , price_data.fdmc
    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on
from seamless_metrics
left join price_data
    on seamless_metrics.date = price_data.date
where true
{{ ez_metrics_incremental('seamless_metrics.date', backfill_date) }}
and seamless_metrics.date < to_date(sysdate())