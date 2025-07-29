{{
    config(
        materialized="incremental",
        database = 'orca',
        schema = 'core',
        snowflake_warehouse = 'ORCA',
        alias = 'ez_metrics',
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

with date_spine as (
    select
        date
    from {{ ref("dim_date_spine") }}
    where date between '2021-04-15' and to_date(sysdate())
)
, fees_and_volume as (
    select 
        date, 
        climate_fund_fees, 
        dao_treasury_fees, 
        lp_fees, 
        total_fees, 
        volume 
    from {{ ref("fact_orca_fees_and_volume") }}
)
, dau_txns as (
    select 
        date, 
        num_swaps, 
        unique_traders 
    from {{ ref("fact_orca_dau_txns") }}
)
, tvl as (
    select t.date, t.tvl from {{ ref("fact_defillama_protocol_tvls") }} t
    join {{ ref("fact_defillama_protocols") }} p on p.id = t.defillama_protocol_id and p.name = 'Orca'
)
, market_data as (
    {{ get_coingecko_metrics('orca')}}
)
, supply_data as (
    select
        date
        , premine_unlocks_native
        , net_change
        , circulating_supply_native
    from {{ ref("fact_orca_supply_data") }}
)
select
    ds.date
    , 'orca' as artemis_id

    -- Standardized Metrics
    -- Market Metrics
    , market_data.price
    , market_data.market_cap
    , market_data.fdmc
    , market_data.token_volume

    -- Usage Metrics
    , coalesce(dau_txns.unique_traders, 0) as spot_dau
    , coalesce(dau_txns.num_swaps, 0) as spot_txns
    , coalesce(fees_and_volume.volume, 0) as spot_volume
    , COALESCE(tvl.tvl, 
        last_value(tvl ignore nulls) over (
            order by date desc rows between unbounded preceding and current row
        )) as tvl
    
    -- Fee Metrics
    , coalesce(fees_and_volume.total_fees, 0) as spot_fees
    , fees_and_volume.total_fees as fees
    , coalesce(fees_and_volume.lp_fees, 0) as lp_fee_allocation
    , coalesce(fees_and_volume.dao_treasury_fees, 0) as treasury_fee_allocation
    , coalesce(fees_and_volume.climate_fund_fees, 0) as other_fee_allocation
    
    -- Financial Statement Metrics
    , fees_and_volume.dao_treasury_fees as revenue

    -- Supply Metrics
    , coalesce(supply_data.premine_unlocks_native, 0) as premine_unlocks_native
    , coalesce(supply_data.net_change, 0) as net_supply_change_native
    , coalesce(supply_data.circulating_supply_native, 0) as circulating_supply_native

    -- Other Metrics
    , market_data.token_turnover_circulating
    , market_data.token_turnover_fdv

    -- Timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on
from date_spine ds
left join fees_and_volume using (date)
left join dau_txns using (date)
left join tvl using (date)
left join supply_data using (date)
left join market_data using (date)
where true
{{ ez_metrics_incremental('ds.date', backfill_date) }}
and ds.date < to_date(sysdate())