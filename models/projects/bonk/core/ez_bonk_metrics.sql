{{
    config(
        materialized='incremental',
        snowflake_warehouse='BONK',
        database='BONK',
        schema='core',
        alias='ez_metrics',
        incremental_strategy="merge",
        unique_key="date",
        on_schema_change="append_new_columns",
        merge_update_columns=var("backfill_columns", []),
        merge_exclude_columns=["created_on"] if not var("backfill_columns", []) else none,
        full_refresh=false,
        tags=["ez_metrics"]
    )
 }}

with date_spine as (
    select date_spine.date
    from {{ ref('dim_date_spine') }} date_spine
    where date_spine.date between '2025-04-25' and (to_date(sysdate()) - 1)
)
, price_data as ({{ get_coingecko_metrics("bonk") }})

, bonk_metrics as (
    select 
        date
        , coins_minted
        , launchpad_dau
        , launchpad_volume
        , launchpad_txns
        , launchpad_fees
    from {{ ref('fact_bonk_fundamental_metrics') }}
)

-- Final combined query with one row per day
select
    date_spine.date
    --Standardized Metrics
    , coins_minted
    , launchpad_dau
    , launchpad_volume
    , launchpad_txns
    -- Market Data Metrics
    , price
    , market_cap
    , fdmc
    , token_volume
    -- timestamp columns
    , sysdate() as created_on
    , sysdate() as modified_on
from date_spine
left join bonk_metrics using(date)
left join price_data using(date)
where true
{{ ez_metrics_incremental('date_spine.date', backfill_date) }}
and date_spine.date < to_date(sysdate())
order by date desc
