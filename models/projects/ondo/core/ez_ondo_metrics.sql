{{
    config(
        materialized="incremental",
        database = 'ondo',
        schema = 'core',
        snowflake_warehouse = 'ONDO',
        alias = 'ez_metrics',
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

with date_spine as (
    select
        date
    from {{ ref("dim_date_spine") }}
    where date between '2023-01-26' and to_date(sysdate())
)
, fees as (
    select date, fee as fees from {{ ref("fact_ondo_ousg_fees") }}
)
, tvl as (
    select
        date,
        sum(tokenized_mcap_change) as tokenized_mcap_change,
        sum(tokenized_mcap) as tokenized_mcap,
    from {{ ref("ez_ondo_metrics_by_chain") }}
    group by 1
)
, ff_defillama_metrics as (
    select
        date,
        avg(tvl) as tvl
    from {{ ref("fact_defillama_protocol_tvls") }}
    where defillama_protocol_id = 2537
    group by 1
)
, supply as (
    select
        date,
        premine_unlocks_native,
        net_supply_change_native,
        circulating_supply_native
    from {{ ref("fact_ondo_daily_supply") }}
)
, market_data as (
    {{ get_coingecko_metrics("ondo-finance") }}
)

select
    ds.date,

    -- Standardized Metrics
    -- Market Metrics
    market_data.price,
    market_data.market_cap,
    market_data.fdmc,
    market_data.token_volume,

    -- Usage Metrics
    coalesce(tvl.tokenized_mcap_change, 0) as tokenized_mcap_change,
    coalesce(tvl.tokenized_mcap, 0) as tokenized_mcap,
    coalesce(ff_defillama_metrics.tvl, 0) as lending_tvl, -- Flux Finance TVL
    coalesce(ff_defillama_metrics.tvl, 0) as tvl,

    -- Fee Metrics
    coalesce(fees.fees, 0) as fees,

    -- Supply Metrics
    coalesce(supply.premine_unlocks_native, 0) as premine_unlocks_native,
    coalesce(supply.net_supply_change_native, 0) as net_supply_change_native,
    coalesce(supply.circulating_supply_native, 0) as circulating_supply_native,

    -- timestamp columns
    TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on,
    TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on
from date_spine ds
left join fees using (date)
left join tvl using (date)
left join ff_defillama_metrics using (date)
left join supply using (date)
left join market_data using (date)
where true
{{ ez_metrics_incremental('ds.date', backfill_date) }}
and ds.date < to_date(sysdate())