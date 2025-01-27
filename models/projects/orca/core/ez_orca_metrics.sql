{{
    config(
        materialized="table",
        database = 'orca',
        schema = 'core',
        snowflake_warehouse = 'ORCA',
        alias = 'ez_metrics'
    )
}}

with date_spine as (
    select
        date
    from {{ ref("dim_date_spine") }}
    where date between '2023-01-26' and to_date(sysdate())
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

select
    ds.date,
    fees_and_volume.climate_fund_fees,
    fees_and_volume.dao_treasury_fees as revenue,
    fees_and_volume.lp_fees as supply_side_revenue,
    fees_and_volume.total_fees as fees,
    fees_and_volume.volume,
    dau_txns.num_swaps as number_of_swaps,
    dau_txns.unique_traders
from date_spine ds
left join fees_and_volume on ds.date = fees_and_volume.date
left join dau_txns on ds.date = dau_txns.date