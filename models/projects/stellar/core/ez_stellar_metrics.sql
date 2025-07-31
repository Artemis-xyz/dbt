{{
    config(
        materialized="incremental",
        snowflake_warehouse="STELLAR",
        database="stellar",
        schema="core",
        alias="ez_metrics",
        incremental_strategy="merge",
        unique_key="date",
        on_schema_change="append_new_columns",
        merge_update_columns=var("backfill_columns", []),
        merge_exclude_columns=["created_on"] if not var("backfill_columns", []) else none,
        full_refresh=var("full_refresh", false),
        tags=["ez_metrics"]
    )
}}

{% set backfill_date = var("backfill_date", None) %}

with fundamental_data as (
    select 
        * EXCLUDE date,
        TO_TIMESTAMP_NTZ(date) AS date 
    from {{ source('PROD_LANDING', 'ez_stellar_metrics') }}
)
, rwa_tvl as (
    select * 
    from {{ ref('fact_stellar_rwa_tvl') }}
)
, stablecoin_tvl as (
    -- Sum mktcap in USD across all stablecoins 
    select 
        date,
        sum(total_circulating_usd) as stablecoin_mc
    from {{ ref ('fact_stellar_stablecoin_tvl') }} 
    group by 
        date 
)
, issued_supply_metrics as (
    select 
        date,
        max_supply as max_supply_native,
        total_supply as total_supply_native,
        issued_supply as issued_supply_native,
        circulating_supply_native as circulating_supply_native
    from {{ ref('fact_stellar_issued_supply_and_float_dbt') }}
)
, prices as ({{ get_coingecko_price_with_latest("stellar") }})
, market_metrics as ( {{ get_coingecko_metrics("stellar") }} )
select
    fundamental_data.date
    , 'stellar' as artemis_id
    , fundamental_data.chain

    -- Standardized Metrics

    -- Market Data
    , price_data.price
    , price_data.market_cap
    , price_data.fdmc
    , price_data.token_volume

    -- Usage Data
    , fundamental_data.dau as chain_dau
    , fundamental_data.dau as dau
    , fundamental_data.wau as chain_wau
    , fundamental_data.mau as chain_mau
    , fundamental_data.txns as chain_txns
    , fundamental_data.txns as txns
    , fundamental_data.returning_users as returning_users
    , fundamental_data.new_users as new_users
    
    -- Fee Data
    , fees_data.fees_native as fees_native
    , fees_data.fees as fees

    -- Issued Supply Metrics
    , issued_supply_metrics.max_supply_native
    , issued_supply_metrics.total_supply_native
    , issued_supply_metrics.issued_supply_native
    , issued_supply_metrics.circulating_supply_native

    -- Financial Statement Metrics
    , fees_data.fees_native as revenue_native
    , fees_data.fees as revenue
    
    -- Stablecoin Metrics
    , stablecoin_mc as stablecoin_total_supply
    , stablecoin_tvl.stablecoin_mc

    -- Token Turnover/Other Data
    , market_metrics.token_turnover_circulating
    , market_metrics.token_turnover_fdv

    -- Bespoke Metrics
    , rwa_tvl.rwa_tvl as tokenized_market_cap
    , null as low_sleep_users
    , null as high_sleep_users
    , null as sybil_users
    , null as non_sybil_users
    , fundamental_data.soroban_txns AS soroban_txns
    , fundamental_data.daily_fees as fees_native
    , fundamental_data.assets_deployed as assets_deployed
    , fundamental_data.operations as operations
    , fundamental_data.active_contracts as active_contracts
    , fundamental_data.ledgers_closed as ledgers_closed

    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on

from fundamental_data
left join prices using(date)
left join market_metrics on fundamental_data.date = market_metrics.date
left join rwa_tvl on fundamental_data.date = rwa_tvl.date
left join stablecoin_tvl on fundamental_data.date = stablecoin_tvl.date
left join issued_supply_metrics on fundamental_data.date = issued_supply_metrics.date
where true
{{ ez_metrics_incremental('fundamental_data.date', backfill_date) }}
and fundamental_data.date < to_date(sysdate())
