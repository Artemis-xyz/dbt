{{
    config(
        materialized='table',
        snowflake_warehouse='ETHENA',
        database='ethena',
        schema='core',
        alias='ez_metrics'
    )
}}

with usde_metrics as (
    select
        date,
        stablecoin_txns,
        stablecoin_dau
    from {{ ref('ez_usde_metrics') }}
)
, ena_metrics as (
    SELECT
        date,
        sum(coalesce(collateral_fees.collateral_fee, 0) + coalesce(yield_fees.fees, 0)) as fees
    FROM  {{ ref('fact_ethena_yield_fees') }} yield_fees
    left join  {{ ref('fact_ethena_collateral_fees') }} collateral_fees using(date)
    group by 1
)
, tvl as (
    SELECT
        date,
        stablecoin_total_supply
    FROM {{ ref('ez_usde_metrics') }}
)
select
    usde_metrics.date,
    usde_metrics.stablecoin_dau as stablecoin_dau,
    usde_metrics.stablecoin_txns as stablecoin_txns,
    coalesce(ena_metrics.fees, 0) as fees,
    tvl.stablecoin_total_supply as tvl
from usde_metrics
left join ena_metrics using(date)
left join tvl using(date)
where usde_metrics.date < to_date(sysdate())
order by 1 desc
