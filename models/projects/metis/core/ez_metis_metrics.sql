{{
    config(
        materialized = "incremental",
        snowflake_warehouse = "METIS",
        database = "METIS",
        schema = "core",
        alias = "ez_metrics",
        incremental_strategy="merge",
        unique_key="date",
        on_schema_change="append_new_columns",
        merge_update_columns=var("backfill_columns", []),
        merge_exclude_columns=["created_on"] | reject('in', var("backfill_columns", [])) | list,
        full_refresh=false,
        tags=["ez_metrics"],
    )
}}

{% set backfill_date = var("backfill_date", None) %}

with fees as (
    select
        date,
        fees_usd
    from {{ref("fact_metis_fees")}}
),
txns as (
    select
        date,
        txns
    from {{ref("fact_metis_txns")}}
)
, daus as (
    select
        date,
        dau
    from {{ref("fact_metis_dau")}}
)
, defillama_data as (
    {{ get_defillama_metrics("metis") }}
)
, supply_data as (
    select
        date
        , premine_unlocks_native
        , net_supply_change_native
        , circulating_supply_native
    from {{ref("fact_metis_supply_data")}}
)
, market_data as ({{ get_coingecko_metrics("metis-token") }})

select
    coalesce(fees.date, txns.date, daus.date) as date
    -- Standardized Metrics
    -- Market Data Metrics
    , market_data.price
    , market_data.market_cap
    , market_data.fdmc
    , market_data.token_volume
    -- Usage Metrics
    , daus.dau as chain_dau
    , daus.dau
    , txns.txns as chain_txns
    , txns.txns
    , defillama_data.dex_volumes as chain_spot_volume
    , defillama_data.tvl as tvl

    -- Fee Metrics
    , fees.fees_usd as chain_fees
    , fees.fees_usd as fees
    , fees.fees_usd * 0.7 as validator_fee_allocation
    , fees.fees_usd * 0.3 as other_fee_allocation
 
    -- Supply Metrics
    , supply_data.premine_unlocks_native
    , supply_data.net_supply_change_native
    , supply_data.circulating_supply_native
 
    -- Other Metrics
    , market_data.token_turnover_circulating
    , market_data.token_turnover_fdv

    -- Timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on
from fees
left join txns USING(date)
left join daus USING(date)
left join market_data USING(date)
left join defillama_data USING(date)
left join supply_data USING(date)
where true
{{ ez_metrics_incremental('fees.date', backfill_date) }}
and fees.date < to_date(sysdate())