{{
    config(
        materialized="table",
        snowflake_warehouse="IOTEX",
        database="iotex",
        schema="core",
        alias="ez_metrics"
    )
}}

with price as (
        select * from ({{ get_coingecko_price_with_latest("iotex") }}) 
)
, metrics as (
    select
        m.date,
        m.dau,
        m.txns,
        m.fees_native,
        m.fees_native * p.price as fees,
        fees as total_supply_side_revenue,
        case when
            m.date > '2023-04-01' 
                then fees * 0.7
                else 0
            end as primary_supply_side_revenue, -- 70% of fees go to validators
        case when
            m.date > '2023-04-01'
                then fees * 0.3
                else fees
            end as secondary_supply_side_revenue -- 30% of fees go to app developers
    from {{ ref("fact_iotex_metrics") }} m
    left join price p on p.date = m.date
)
, supply as (
    select
        s.date,
        s.burn,
        s.mints,
        s.circulating_supply,
        s.burn * p.price as burn_usd,
        s.mints * p.price as mints_usd
    from {{ ref("fact_iotex_supply") }} s
    left join price p on p.date = s.date
)
, defillama_tvl as (
    SELECT
        date,
        tvl
    FROM pc_dbt_db.prod.fact_defillama_chain_tvls
    where defillama_chain_name ILIKE 'iotex'
)
, defillama_dex_volume as (
    SELECT
        date,
        dex_volumes
    FROM pc_dbt_db.prod.fact_defillama_chain_dex_volumes
    where defillama_chain_name ILIKE 'iotex'
)
, date_spine as (
    select
        date
    from {{ ref("dim_date_spine") }}
    where date between '2021-03-17' and to_date(sysdate())
)
, market_metrics as ({{ get_coingecko_metrics("iotex") }})

select
    metrics.date,
    'iotex' as chain
    , metrics.dau
    , metrics.txns
    , metrics.fees
    , metrics.total_supply_side_revenue
    , metrics.primary_supply_side_revenue
    , metrics.secondary_supply_side_revenue
    , supply.burn_usd as revenue
    , supply.mints_usd as gross_emissions
    , supply.mints as gross_emissions_native
    , supply.circulating_supply as circulating_supply
    , supply.burn AS burned_fee_allocation_native


    -- Standardized Metrics

    -- Market Metrics
    , market_metrics.price
    , market_metrics.market_cap
    , market_metrics.fdmc

    -- Usage Metrics
    , metrics.dau as chain_dau
    , metrics.txns as chain_txns
    , defillama_dex_volume.dex_volumes as chain_spot_volume
    , defillama_tvl.tvl as chain_tvl

    -- Cashflow Metrics
    , metrics.fees AS ecosystem_revenue
    , metrics.primary_supply_side_revenue AS validator_fee_allocation
    , metrics.secondary_supply_side_revenue AS service_fee_allocation
    , supply.burn_usd AS burned_fee_allocation

    -- IOTX Token Supply Data
    , 0 as emissions_native
    , supply.mints as premine_unlocks_native
    , supply.burn AS burns_native
    , 0 + coalesce(supply.mints, 0) - coalesce(supply.burn, 0) AS net_supply_change_native
    , sum(0 + coalesce(supply.mints, 0) - coalesce(supply.burn, 0)) over (order by supply.date) AS circulating_supply_native

from date_spine
left join market_metrics using (date)
left join defillama_dex_volume using (date)
left join defillama_tvl using (date)
left join metrics using (date)
left join supply using (date)
where date_spine.date < to_date(sysdate())