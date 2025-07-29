{{
    config(
        materialized="incremental",
        snowflake_warehouse="MANTA",
        database="manta",
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
    market_data as ({{ get_coingecko_metrics("manta-network") }})
   , defillama_data as ({{ get_defillama_metrics("manta") }})
   , supply_data as (
        select
            date
            , gross_emissions_native
            , premine_unlocks_native
            , net_supply_change_native
            , circulating_supply_native
        from {{ ref('fact_manta_supply_data') }}
    )
    , fundamentals as (
        select
            date
            , fees
            , txns
            , dau
        from {{ ref('fact_manta_txns_daa') }}
    )
SELECT
    date
    -- Standardized Metrics
    -- Market Data Metrics
    , market_data.price
    , market_data.market_cap
    , market_data.fdmc
    , market_data.token_volume

    -- Usage Metrics
    , f.txns AS chain_txns
    , f.txns as txns
    , f.dau AS chain_dau
    , f.dau
    , defillama_data.dex_volumes as chain_spot_volume
    , defillama_data.tvl as tvl

    -- Fee Metrics
    , f.fees as chain_fees
    , f.fees
    , case when date > '2024-09-30' then f.fees / 2 else f.fees end as foundation_fee_allocation
    , case when date > '2024-09-30' then f.fees / 2 else 0 end as other_fee_allocation

    -- Supply Data
    , supply_data.gross_emissions_native
    , supply_data.premine_unlocks_native
    , supply_data.net_supply_change_native
    , supply_data.circulating_supply_native
    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on
FROM fundamentals f
LEFT JOIN market_data using (date)
LEFT JOIN defillama_data using (date)
LEFT JOIN supply_data using (date)
where true
{{ ez_metrics_incremental('date', backfill_date) }}
and date < to_date(sysdate())
