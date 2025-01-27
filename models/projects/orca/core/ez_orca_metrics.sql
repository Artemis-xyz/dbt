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

select
    ds.date,
    fees_and_volume.climate_fund_fees,
    fees_and_volume.dao_treasury_fees as revenue,
    fees_and_volume.lp_fees as supply_side_revenue,
    fees_and_volume.total_fees as fees,
    fees_and_volume.volume,
    dau_txns.num_swaps as number_of_swaps,
    dau_txns.unique_traders,
    COALESCE(tvl.tvl, 
        last_value(tvl ignore nulls) over (
            order by date desc rows between unbounded preceding and current row
        )) as tvl
from date_spine ds
left join fees_and_volume using (date)
left join dau_txns using (date)
left join tvl using (date)