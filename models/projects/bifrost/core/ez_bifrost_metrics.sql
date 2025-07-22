{{
    config(
        materialized="incremental",
        snowflake_warehouse="BIFROST",
        database="bifrost",
        schema="core",
        alias="ez_metrics",
        incremental_strategy="merge",
        unique_key="date",
        on_schema_change="append_new_columns",
        merge_exclude_columns=["created_on"],
        full_refresh=false
    )
}}

-- NOTE: When running a backfill, add merge_update_columns=[<columns>] to the config and set the backfill date below

{% set backfill_date = None %}

with
    fundamental_data as (
        select
            date, 
            txns,
            daa, 
            fees_native, 
            fees_usd
        from {{ ref("fact_bifrost_fundamental_metrics") }}
        {{ ez_metrics_incremental('date', backfill_date) }}
    ),
    price_data as ({{ get_coingecko_metrics('bifrost-native-coin') }})
select
    f.date
    , txns
    , daa as dau
    , coalesce(fees_native, 0) as fees_native
    , coalesce(fees_usd, 0) as fees
    -- Standardized Metrics
    -- Market Data
    , price_data.price
    , price_data.market_cap
    , price_data.fdmc
    , price_data.token_volume
    -- Chain Metrics
    , txns as chain_txns
    , daa as chain_dau
    -- Cash Flow Metrics
    , fees as ecosystem_revenue
    , fees_native as ecosystem_revenue_native
    , price_data.token_turnover_circulating
    , price_data.token_turnover_fdv
    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on
from fundamental_data f
left join price_data using(f.date)
where f.date < to_date(sysdate())
