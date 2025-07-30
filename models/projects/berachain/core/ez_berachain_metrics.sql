{{
    config(
        materialized="incremental",
        snowflake_warehouse="BERACHAIN",
        database="berachain",
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
     price_data as ({{ get_coingecko_metrics('berachain-bera') }}),
     dex_volumes as (
        select date, daily_volume as dex_volumes, daily_volume_adjusted as adjusted_dex_volumes
        from {{ ref("fact_berachain_daily_dex_volumes") }}
     ),
     supply_data as (
        select 
            date
            , premine_unlocks_native
            , emission_native
            , burns_native
            , net_supply_change_native
            , circulating_supply_native
        from {{ ref('fact_berachain_supply_data') }}
     )
     , fundamental_metrics as (
        select * from {{ ref("fact_berachain_fundamental_metrics") }}
     )
select
    f.date
    , 'berachain' as artemis_id

    --Market Data
    , price
    , market_cap as mc
    , fdmc
    , token_volume

    --Usage Data
    , daa
    , txns
    , dex_volumes as spot_volume
    , adjusted_dex_volumes as adjusted_spot_volume

    --Fee Data
    , fees_native
    , fees

    --Fee Allocation
    , burns_native as burned_fee_allocation_native

    --Financial Statements
    , burns_native as revenue_native
    , burns_native * price as revenue

    --Supply Data
    , premine_unlocks_native
    , emission_native as gross_emissions_native
    , circulating_supply_native

    --TOKEN TURNOVER/OTHER DATA
    , token_turnover_circulating
    , token_turnover_fdv

    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on

from {{ ref("fact_berachain_fundamental_metrics") }} as f
left join price_data on f.date = price_data.date
left join dex_volumes on f.date = dex_volumes.date
left join supply_data on f.date = supply_data.date
where true
{{ ez_metrics_incremental('f.date', backfill_date) }}
and f.date  < to_date(sysdate())