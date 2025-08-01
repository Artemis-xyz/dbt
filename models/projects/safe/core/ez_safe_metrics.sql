{{
    config(
        materialized="incremental",
        database = 'SAFE',
        schema = 'core',
        snowflake_warehouse = 'SAFE',
        alias = 'ez_metrics',
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

with txns as (
    select
        date
        , txns
    from {{ ref("fact_safe_txns") }}
)
, safes_created as (
    select
        date
        , safes_created
    from {{ ref("fact_safe_daily_safes_created") }}
)
, tvl as (
    select
        date
        , sum(tvl) as tvl
    from {{ ref("fact_safe_tvl_by_chain") }}
    group by date
)
, supply_data as (
    select
        date
        , gross_emissions_native
        , premine_unlocks_native
        , burns_native
        , net_supply_change_native
        , circulating_supply_native
    from {{ ref("fact_safe_supply_data") }}
)

, market_data as (
    {{ get_coingecko_metrics('safe') }}
)

select
    txns.date
    , txns
    , safes_created as multisigs_created
    , tvl as value_in_multisigs

    -- Standardized Metrics
    , price
    , market_cap
    , fdmc
    , token_volume
    , token_turnover_circulating
    , token_turnover_fdv

    -- Supply Metrics
    , gross_emissions_native
    , premine_unlocks_native
    , burns_native
    , net_supply_change_native
    , circulating_supply_native

    -- timestamp columns
    , sysdate() as created_on
    , sysdate() as modified_on
from txns
left join safes_created using (date)
left join tvl using (date)
left join market_data using (date)
left join supply_data using (date)
where true
{{ ez_metrics_incremental('date', backfill_date) }}
and date < to_date(sysdate())