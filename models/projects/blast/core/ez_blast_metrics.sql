-- depends_on {{ ref("fact_blast_transactions_v2") }}
{{
    config(
        materialized="incremental",
        snowflake_warehouse="blast",
        database="blast",
        schema="core",
        alias="ez_metrics",
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

with
    fundamental_data as ({{ get_goldsky_chain_fundamental_metrics("blast", "_v2") }})
    , defillama_data as ({{ get_defillama_metrics("blast") }})
    , market_data as ({{ get_coingecko_metrics("blast") }})
    , eth_price as ({{ get_coingecko_metrics("ethereum") }})
    , contract_data as ({{ get_contract_metrics("blast") }})
    -- NOTE, this says l1 data cost, but that's inaccurate 
    -- its both data and execution cost, but I'm following convention for now and we don't publish 
    -- this field anywhere, we only use it to derive revenue
    , expenses_data as (
        select date, chain, l1_data_cost_native, l1_data_cost
        from {{ ref("fact_blast_l1_data_cost") }}
    )  -- supply side revenue and fees
    , rolling_metrics as ({{ get_rolling_active_address_metrics("blast") }})
    , blast_dex_volumes as (
        select date, daily_volume as dex_volumes, daily_volume_adjusted as adjusted_dex_volumes
        from {{ ref("fact_blast_daily_dex_volumes") }}
    ),
    premine_emissions as (
        select date, premine_unlocks_native, circulating_supply_native
        from {{ ref("fact_blast_daily_supply_data") }}
    )
    , date_spine as (
        select date 
        from {{ ref("dim_date_spine") }}
        where date >= (SELECT MIN(date) FROM fundamental_data UNION 
                        SELECT MIN(date) FROM defillama_data UNION 
                        SELECT MIN(date) FROM contract_data UNION 
                        SELECT MIN(date) FROM expenses_data)
        and date < to_date(sysdate())
    )
select
    date_spine.date as date
    , 'blast' as artemis_id

    -- Standardized Metrics

    -- Market Data 
    , market_data.price
    , market_data.market_cap
    , market_data.fdmc
    , market_data.token_volume as token_volume

    -- Usage Data
    , daa AS chain_dau
    , wau AS chain_wau
    , mau AS chain_mau
    , daa AS dau
    , txns AS chain_txns
    , txns 
    , tvl AS chain_tvl
    , tvl
    , dune_dex_volumes_blast.dex_volumes as chain_spot_volume
    , dune_dex_volumes_blast.adjusted_dex_volumes as chain_spot_volume_adjusted

    -- Fee Data
    , fees_native                                                                              
    , fees_native * eth_price.price AS fees
    , l1_data_cost AS l1_fee_allocation
    , (fees_native * eth_price.price) - l1_data_cost AS foundation_fee_allocation

    -- Financial Statements
    , 0 as revenue

    -- Supply Metrics
    , premine_unlocks_native
    , circulating_supply_native

    -- Developer metrics
    , weekly_contracts_deployed
    , weekly_contract_deployers
    -- Supply Metrics
    , premine_unlocks_native
    , premine_unlocks_native as net_supply_change_native
    , circulating_supply_native
    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on
from fundamental_data
left join defillama_data on fundamental_data.date = defillama_data.date
left join contract_data on fundamental_data.date = contract_data.date
left join expenses_data on fundamental_data.date = expenses_data.date
left join rolling_metrics on fundamental_data.date = rolling_metrics.date
left join blast_dex_volumes as dune_dex_volumes_blast on fundamental_data.date = dune_dex_volumes_blast.date
left join price_data on fundamental_data.date = price_data.date
left join eth_price on fundamental_data.date = eth_price.date
left join premine_emissions on fundamental_data.date = premine_emissions.date
where true
{{ ez_metrics_incremental('fundamental_data.date', backfill_date) }}
and fundamental_data.date < to_date(sysdate())

    -- Turnover data
    , market_data.token_turnover_circulating as token_turnover_circulating
    , market_data.token_turnover_fdv as token_turnover_fdv
from date_spine
left join fundamental_data using(date)
left join defillama_data using(date)
left join contract_data using(date)
left join expenses_data using(date)
left join rolling_metrics using(date)
left join blast_dex_volumes as dune_dex_volumes_blast using(date)
left join market_data using(date)
left join eth_price using(date)
left join premine_emissions using(date)
