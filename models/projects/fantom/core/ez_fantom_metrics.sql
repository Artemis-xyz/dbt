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
    , price_data.price
    , price_data.market_cap as mc
    , price_data.fdmc
    , price_data.token_volume

    --Usage Data
    , daa_gold.daa as chain_dau
    , daa_gold.daa as dau
    , rolling_metrics.wau as chain_wau
    , rolling_metrics.wau as wau
    , rolling_metrics.mau as chain_mau
    , rolling_metrics.mau as mau
    , txns_gold.txns as chain_txns
    , txns_gold.txns
    , gas_gold.avg_txn_fee as chain_avg_txn_fee
    , gas_gold.avg_txn_fee
    , fantom_dex_volumes.dex_volumes as spot_volume
    , fantom_dex_volumes.adjusted_dex_volumes as spot_volume_adjusted

    --Fee Data
    , gas_gold.fees_native as fees_native
    , gas_gold.fees as fees

    --Fee Allocation
    , gas_gold.fees as foundation_fee_allocation

    --Financial Statements
    , gas_gold.fees_native as revenue_native
    , gas_gold.fees as revenue

    -- Developer Metrics
    , contract_data.weekly_contracts_deployed
    , contract_data.weekly_contract_deployers

    --TOKEN TURNOVER/OTHER DATA
    , price_data.token_turnover_circulating
    , price_data.token_turnover_fdv

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
