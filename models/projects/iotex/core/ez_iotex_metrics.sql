{{
    config(
        materialized="table",
        snowflake_warehouse="IOTEX",
        database="iotex",
        schema="core",
        alias="ez_metrics"
    )
}}

with
    price as (
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
    ),
    supply as (
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
    , tvl as (
        SELECT
            date,
            tvl
        FROM pc_dbt_db.prod.fact_defillama_chain_tvls
        where defillama_chain_name ILIKE 'iotex'
    )
    , dex_volume as (
        SELECT
            date,
            dex_volumes
        FROM pc_dbt_db.prod.fact_defillama_chain_dex_volumes
        where defillama_chain_name ILIKE 'iotex'
    )
select
    metrics.date,
    'iotex' as chain,
    metrics.dau,
    metrics.txns,
    metrics.fees,
    metrics.total_supply_side_revenue,
    metrics.primary_supply_side_revenue,
    metrics.secondary_supply_side_revenue,
    supply.burn_usd as revenue,
    tvl.tvl,
    dex_volume.dex_volumes,
    supply.burn as burns_native,
    supply.mints as mints_native,
    supply.mints_usd,
    supply.circulating_supply
from metrics
left join supply using (date)
left join tvl using (date)
left join dex_volume using (date)
where metrics.date < to_date(sysdate())