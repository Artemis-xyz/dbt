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
    , price_data.price
    , price_data.market_cap as mc
    , price_data.fdmc
    , price_data.token_volume

    --Usage Data
    , fundamental_metrics.daa as dau
    , fundamental_metrics.txns
    , dex_volumes.dex_volumes as spot_volume
    , dex_volumes.adjusted_dex_volumes as adjusted_spot_volume

    --Fee Data
    , fundamental_metrics.fees_native
    , fundamental_metrics.fees

    --Fee Allocation
    , supply_data.burns_native as burned_fee_allocation_native

    --Financial Statements
    , supply_data.burns_native as revenue_native
    , supply_data.burns_native * price_data.price as revenue

    --Supply Data
    , supply_data.premine_unlocks_native
    , supply_data.emission_native as gross_emissions_native
    , supply_data.circulating_supply_native

    --TOKEN TURNOVER/OTHER DATA
    , price_data.token_turnover_circulating
    , price_data.token_turnover_fdv

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