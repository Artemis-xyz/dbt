--depends_on: {{ ref("fact_fantom_rolling_active_addresses") }}
{{
    config(
        materialized="incremental",
        snowflake_warehouse="FANTOM",
        database="fantom",
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
    daa_gold as (
        select
            date, chain, daa
        from {{ ref("fact_fantom_daa") }}
    ),
    txns_gold as (
        select
            date, chain, txns
        from {{ ref("fact_fantom_txns") }}
    ),
    gas_gold as (
        select
            date, chain, gas, gas_usd, fees, revenue
        from {{ ref("fact_fantom_gas_gas_usd_fees_revenue") }}
    ),
    contract_data as ({{ get_contract_metrics("fantom") }}),
    rolling_metrics as ({{ get_rolling_active_address_metrics("fantom") }}),
    fantom_dex_volumes as (
        select date, daily_volume as dex_volumes, daily_volume_adjusted as adjusted_dex_volumes
        from {{ ref("fact_fantom_daily_dex_volumes") }}
    ),
    price_data as ({{ get_coingecko_metrics("fantom") }})
select
    d.date
    , 'fantom' as artemis_id

    -- Market Data
    , price
    , market_cap
    , fdmc
    , token_volume

    --Usage Data
    , dau as chain_dau
    , dau
    , wau as chain_wau
    , wau
    , mau as chain_mau
    , mau
    , txns as chain_txns
    , txns
    , avg_txn_fee as chain_avg_txn_fee
    , avg_txn_fee
    , dex_volumes as spot_volume
    , adjusted_dex_volumes as spot_volume_adjusted

    --Fee Data
    , fees_native as fees_native
    , fees as fees

    --Fee Allocation
    , fees as foundation_fee_allocation

    --Financial Statements
    , fees_native as revenue_native
    , fees as revenue

    -- Developer Metrics
    , weekly_contracts_deployed
    , weekly_contract_deployers

    --TOKEN TURNOVER/OTHER DATA
    , token_turnover_circulating
    , token_turnover_fdv

    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on
from daa_gold d
left join price_data using (d.date)
left join contract_data using (d.date)
left join txns_gold using (d.date)
left join gas_gold using (d.date)
left join rolling_metrics using (d.date)
left join fantom_dex_volumes using (d.date)
where true
{{ ez_metrics_incremental('d.date', backfill_date) }}
and d.date < to_date(sysdate())
