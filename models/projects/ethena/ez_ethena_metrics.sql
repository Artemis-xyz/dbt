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
    select *
    from {{ ref('ez_stablecoin_metrics') }}
    where symbol = 'USDe'
)

select
    usde_metrics.date,
    sum(usde_metrics.stablecoin_dau) as stablecoin_dau,
    sum(usde_metrics.stablecoin_daily_txns) as stablecoin_txns,
    sum(coalesce(collateral_fees.collateral_fee, 0) + coalesce(yield_fees.fees, 0)) as fees
from usde_metrics
left join {{ ref('fact_ethena_yield_fees') }} yield_fees using (date)
left join  {{ ref('fact_ethena_collateral_fees') }} collateral_fees using (date)
where usde_metrics.date < to_date(sysdate())
group by 1
