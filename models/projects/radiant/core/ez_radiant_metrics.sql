{{
    config(
        materialized="incremental",
        snowflake_warehouse="RADIANT",
        database="radiant",
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
    radiant_by_chain as (
        {{
            dbt_utils.union_relations(
                relations=[
                    ref("fact_radiant_arbitrum_borrows_deposits_gold"),
                    ref("fact_radiant_bsc_borrows_deposits_gold"),
                    ref("fact_radiant_ethereum_borrows_deposits_gold"),
                ],
            )
        }}
    )
    , token_incentives as (
        select date, sum(amount_native) as amount_native, sum(amount_usd) as amount_usd 
        from {{ ref("fact_radiant_token_incentives") }}
        group by 1
    )
    , radiant_metrics as (
        select
            date
            , sum(daily_borrows_usd) as daily_borrows_usd
            , sum(daily_supply_usd) as daily_supply_usd
        from radiant_by_chain
        group by 1
    )
    , price_data as ({{ get_coingecko_metrics("radiant") }})

select
    token_incentives.date
    , 'radiant' as app
    , 'DeFi' as category
    , radiant_metrics.daily_borrows_usd
    , radiant_metrics.daily_supply_usd
    -- Standardized metrics

    , radiant_metrics.daily_borrows_usd as lending_loans
    , radiant_metrics.daily_supply_usd as lending_deposits
    , price_data.price
    , price_data.market_cap
    , price_data.fdmc

     -- Supply data
    , token_incentives.amount_native as gross_emissions_native
    , token_incentives.amount_usd as gross_emissions

    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on
from token_incentives
left join price_data
    on token_incentives.date = price_data.date
left join radiant_metrics
    on token_incentives.date = radiant_metrics.date
where true
{{ ez_metrics_incremental('token_incentives.date', backfill_date) }}
and token_incentives.date < to_date(sysdate())