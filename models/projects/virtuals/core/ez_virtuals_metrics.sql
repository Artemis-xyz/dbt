{{
    config(
        materialized="incremental",
        snowflake_warehouse = 'VIRTUALS',
        database='VIRTUALS',
        schema='core',
        alias='ez_metrics',
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

with date_spine as (
    select date
    from {{ ref("dim_date_spine") }}
    where date between '2024-09-10' and to_date(sysdate())
)
, virtuals_daily_agents as (
    select
        date
        , coalesce(daily_agents, 0) as daily_agents
    from {{ ref("fact_virtuals_daily_agents") }}
)
, virtuals_dau as (
    select
        date
        , coalesce(dau, 0) as dau
    from {{ ref("fact_virtuals_dau") }}
)
, virtuals_volume as (
    select
        date
        , coalesce(volume_native, 0) as volume_native
        , coalesce(volume_usd, 0) as volume_usd
    from {{ ref("fact_virtuals_volume") }}
)
, virtuals_fees as (
    select
        date
        , coalesce(fee_fun_native, 0) as fee_fun_native
        , coalesce(fee_fun_usd, 0) as fee_fun_usd
        , coalesce(tax_usd, 0) as tax_usd
        , coalesce(fees, 0) as fees
    from {{ ref("fact_virtuals_fees") }}
)
, market_metrics as (
    {{ get_coingecko_metrics('virtual-protocol') }}
)
, virtuals_supply_data as (
    select
        date
        , virtuals_sablier_lockup
        , virtuals_treasury
        , premine_unlocks_native
        , net_supply_change_native
        , circulating_supply_native
    from {{ ref("fact_virtuals_supply_data") }}
)

select
    date_spine.date
    , 'virtuals' as artemis_id

    -- Standardized Metrics

    -- Market Data
    , market_metrics.price
    , market_metrics.token_volume
    , market_metrics.market_cap
    , market_metrics.fdmc

    -- Usage Data
    , virtuals_dau.dau as spot_dau
    , virtuals_dau.dau as dau
    , virtuals_volume.volume_usd as spot_volume

    -- Fee Data
    , virtuals_fees.tax_usd + virtuals_fees.fee_fun_usd as spot_fees
    , virtuals_fees.fee_fun_usd + virtuals_fees.tax_usd as fees
    , virtuals_fees.fee_fun_usd as service_fee_allocation
    , virtuals_fees.tax_usd as treasury_fee_allocation

    -- Financial Statement
    , virtuals_fees.tax_usd as revenue

    -- Supply Metrics
    , virtuals_supply_data.premine_unlocks_native
    , virtuals_supply_data.circulating_supply_native
    
    -- Turnover Metrics
    , market_metrics.token_turnover_circulating
    , market_metrics.token_turnover_fdv

    -- Bespoke Metrics
    , virtuals_daily_agents.daily_agents

    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on

from date_spine
left join virtuals_daily_agents using (date)
left join virtuals_dau using (date)
left join virtuals_volume using (date)
left join virtuals_fees using (date)
left join virtuals_supply_data using (date)
where true
{{ ez_metrics_incremental('date_spine.date', backfill_date) }}
and date_spine.date < to_date(sysdate())
