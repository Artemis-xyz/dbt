-- depends_on: {{ ref("fact_polkadot_rolling_active_addresses") }}
{{
    config(
        materialized="incremental",
        snowflake_warehouse="POLKADOT",
        database="polkadot",
        schema="core",
        alias="ez_metrics",
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

with
    fundamental_data as (
        select
            date,
            chain,
            txns,
            dau,
            fees_native,
            fees_usd as fees,
            burns,
            fees_usd * .8 as revenue,
            fees_native * .8 as revenue_native
        from {{ ref("fact_polkadot_fundamental_metrics") }}
    ),
    collectives_fundamental_data as (
        select
            date, 
            txns,
            dau, 
            fees_native, 
            fees_usd as fees
        from {{ ref("fact_collectives_fundamental_metrics") }}
    ),
    people_fundamental_data as (
        select
            date, 
            txns,
            dau, 
            fees_native, 
            fees_usd as fees
        from {{ ref("fact_people_fundamental_metrics") }}
    ),
    coretime_fundamental_data as (
        select
            date, 
            txns,
            dau, 
            fees_native, 
            fees_usd as fees
        from {{ ref("fact_coretime_fundamental_metrics") }}
    ),
    bridgehub_fundamental_data as (
        select
            date, 
            txns,
            dau, 
            fees_native, 
            fees_usd as fees
        from {{ ref("fact_polkadot_bridgehub_fundamental_metrics") }}
    ),
    asset_hub_fundamental_data as (
        select
            date,
            txns,
            dau, 
            fees_native, 
            fees_usd as fees
        from {{ ref("fact_polkadot_asset_hub_fundamental_metrics") }}
    ),
    issued_supply_metrics as (
        select 
            date,
            max_supply_to_date as max_supply_native,
            total_supply_to_date as total_supply_native,
            issued_supply as issued_supply_native,
            float as circulating_supply_native
        from {{ ref("fact_polkadot_issued_supply_and_float") }}
    ),
    price_data as ({{ get_coingecko_metrics("polkadot") }}),
    defillama_data as ({{ get_defillama_metrics("polkadot") }}),
    github_data as ({{ get_github_metrics("polkadot") }}),
    rolling_metrics as ({{ get_rolling_active_address_metrics("polkadot") }})
select
    fundamental_data.date
    , 'polkadot' as artemis_id

    --Market Data 
    , price_data.price
    , price_data.market_cap as mc
    , price_data.fdmc

    --Chain Data
    , fundamental_data.dau + collectives_fundamental_data.dau + people_fundamental_data.dau + coretime_fundamental_data.dau + bridgehub_fundamental_data.dau + asset_hub_fundamental_data.dau as chain_dau
    , fundamental_data.dau + collectives_fundamental_data.dau + people_fundamental_data.dau + coretime_fundamental_data.dau + bridgehub_fundamental_data.dau + asset_hub_fundamental_data.dau as dau
    , wau AS chain_wau
    , wau
    , mau AS chain_mau
    , mau
    , fundamental_data.txns + collectives_fundamental_data.txns + people_fundamental_data.txns + coretime_fundamental_data.txns + bridgehub_fundamental_data.txns + asset_hub_fundamental_data.txns as chain_txns
    , fundamental_data.txns + collectives_fundamental_data.txns + people_fundamental_data.txns + coretime_fundamental_data.txns + bridgehub_fundamental_data.txns + asset_hub_fundamental_data.txns as txns
    , (fundamental_data.fees + collectives_fundamental_data.fees + people_fundamental_data.fees + coretime_fundamental_data.fees + bridgehub_fundamental_data.fees + asset_hub_fundamental_data.fees) / (fundamental_data.txns + collectives_fundamental_data.txns + people_fundamental_data.txns + coretime_fundamental_data.txns + bridgehub_fundamental_data.txns + asset_hub_fundamental_data.txns) as chain_avg_txn_fee 
    , (fundamental_data.fees + collectives_fundamental_data.fees + people_fundamental_data.fees + coretime_fundamental_data.fees + bridgehub_fundamental_data.fees + asset_hub_fundamental_data.fees) / (fundamental_data.txns + collectives_fundamental_data.txns + people_fundamental_data.txns + coretime_fundamental_data.txns + bridgehub_fundamental_data.txns + asset_hub_fundamental_data.txns) as avg_txn_fee
    , tvl
   
    --Fee Data
    , fundamental_data.fees_native + collectives_fundamental_data.fees_native + people_fundamental_data.fees_native + coretime_fundamental_data.fees_native + bridgehub_fundamental_data.fees_native + asset_hub_fundamental_data.fees_native as fees_native
    , fundamental_data.fees + collectives_fundamental_data.fees + people_fundamental_data.fees + coretime_fundamental_data.fees + bridgehub_fundamental_data.fees + asset_hub_fundamental_data.fees as chain_fees
    , fundamental_data.fees + collectives_fundamental_data.fees + people_fundamental_data.fees + coretime_fundamental_data.fees + bridgehub_fundamental_data.fees + asset_hub_fundamental_data.fees as fees

    --Fee Allocation
    , fundamental_data.fees_native + collectives_fundamental_data.fees_native + people_fundamental_data.fees_native + coretime_fundamental_data.fees_native + bridgehub_fundamental_data.fees_native + asset_hub_fundamental_data.fees_native as treasury_fee_allocation_native
    , fundamental_data.fees + collectives_fundamental_data.fees + people_fundamental_data.fees + coretime_fundamental_data.fees + bridgehub_fundamental_data.fees + asset_hub_fundamental_data.fees as treasury_fee_allocation

    --Financial Statements
    , fundamental_data.fees_native + collectives_fundamental_data.fees_native + people_fundamental_data.fees_native + coretime_fundamental_data.fees_native + bridgehub_fundamental_data.fees_native + asset_hub_fundamental_data.fees_native as revenue_native
    , fundamental_data.fees + collectives_fundamental_data.fees + people_fundamental_data.fees + coretime_fundamental_data.fees + bridgehub_fundamental_data.fees + asset_hub_fundamental_data.fees as revenue

    -- Issued Supply Metrics
    , issued_supply_metrics.max_supply_native
    , issued_supply_metrics.total_supply_native
    , issued_supply_metrics.issued_supply_native
    , issued_supply_metrics.circulating_supply_native

    -- Developer Metrics
    , weekly_commits_core_ecosystem
    , weekly_commits_sub_ecosystem
    , weekly_developers_core_ecosystem
    , weekly_developers_sub_ecosystem

    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on
from fundamental_data
left join collectives_fundamental_data on fundamental_data.date = collectives_fundamental_data.date
left join people_fundamental_data on fundamental_data.date = people_fundamental_data.date
left join coretime_fundamental_data on fundamental_data.date = coretime_fundamental_data.date
left join bridgehub_fundamental_data on fundamental_data.date = bridgehub_fundamental_data.date
left join asset_hub_fundamental_data on fundamental_data.date = asset_hub_fundamental_data.date
left join price_data on fundamental_data.date = price_data.date
left join defillama_data on fundamental_data.date = defillama_data.date
left join github_data on fundamental_data.date = github_data.date
left join rolling_metrics on fundamental_data.date = rolling_metrics.date
left join issued_supply_metrics on fundamental_data.date = issued_supply_metrics.date
where true
{{ ez_metrics_incremental('fundamental_data.date', backfill_date) }}
and fundamental_data.date < to_date(sysdate())
