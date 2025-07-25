{{
    config(
        materialized="incremental",
        snowflake_warehouse="venus",
        database="venus",
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
    venus_by_chain as (
        {{
            dbt_utils.union_relations(
                relations=[
                    ref("fact_venus_v4_lending_bsc_gold"),
                ],
            )
        }}
    )
    , venus_metrics as (
        select
            date
            , sum(daily_borrows_usd) as daily_borrows_usd
            , sum(daily_supply_usd) as daily_supply_usd
        from venus_by_chain
        group by 1
    )

    , token_incentives as (
        select
            date,
            token_incentives as token_incentives
        from {{ref('fact_venus_token_incentives')}}
    )
    , price_data as ({{ get_coingecko_metrics("venus") }})

select
    venus_metrics.date
    , 'venus' as app
    , 'DeFi' as category
    , venus_metrics.daily_borrows_usd
    , venus_metrics.daily_supply_usd
    -- Standardized metrics
    , venus_metrics.daily_borrows_usd as lending_loans
    , venus_metrics.daily_supply_usd as lending_deposits
    , price_data.price
    , price_data.market_cap
    , price_data.fdmc
    , coalesce(token_incentives.token_incentives, 0) as token_incentives
    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on
from venus_metrics
left join token_incentives
    on venus_metrics.date = token_incentives.date
left join price_data
    on venus_metrics.date = price_data.date
where true
{{ ez_metrics_incremental('venus_metrics.date', backfill_date) }}
and venus_metrics.date < to_date(sysdate())