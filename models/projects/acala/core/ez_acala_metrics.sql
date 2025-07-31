--depends_on: {{ ref("fact_acala_rolling_active_addresses") }}
{{
    config(
        materialized="incremental",
        snowflake_warehouse="ACALA",
        database="acala",
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
    fundamental_data as (
        select
            date, chain, daa, txns, fees_native, fees_usd as fees, fees_native * .2 as revenue_native, fees_usd * .2 as revenue
        from {{ ref("fact_acala_fundamental_metrics") }}
    ),
    rolling_metrics as ({{ get_rolling_active_address_metrics("acala") }}),
    price_data as ({{ get_coingecko_metrics("acala") }})
select
    fundamental_data.date
    , 'acala' as artemis_id

    --Market Data
    , price_data.price
    , price_data.market_cap as mc
    , price_data.fdmc

    --Usage Data
    , fundamental_data.daa AS chain_dau
    , fundamental_data.daa AS dau
    , rolling_metrics.wau AS chain_wau
    , rolling_metrics.wau AS wau
    , rolling_metrics.mau AS chain_mau
    , rolling_metrics.mau AS mau
    , fundamental_data.txns AS chain_txns
    , fundamental_data.txns AS txns

    --Fee Data
    , fundamental_data.fees_native
    , fundamental_data.fees

    --Fee Allocation
    , fundamental_data.fees_native AS burned_fee_allocation

    --Financial Statements
    , fundamental_data.fees_native AS revenue_native 
    , fundamental_data.fees AS revenue

    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on
from fundamental_data
left join rolling_metrics on fundamental_data.date = rolling_metrics.date
left join price_data on fundamental_data.date = price_data.date
where true
{{ ez_metrics_incremental("fundamental_data.date", backfill_date) }}
and fundamental_data.date < to_date(sysdate())

