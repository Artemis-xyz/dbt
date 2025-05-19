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
, ena_cashflow as (
    SELECT
        date,
        service_cash_flow,
        foundation_cash_flow
    FROM {{ ref('fact_ethena_yield_fees') }}
)
, tvl as (
    SELECT
        date,
        stablecoin_total_supply
    FROM {{ ref('ez_usde_metrics') }}
)
, supply_data as (
    select *
    from {{ ref('fact_ethena_supply') }}
)
select
    usde_metrics.date,
    usde_metrics.stablecoin_dau as stablecoin_dau,
    usde_metrics.stablecoin_txns as stablecoin_txns,
    coalesce(ena_metrics.fees, 0) as fees,
    coalesce(ena_metrics.fees, 0) as ecosystem_revenue,
    coalesce(ena_cashflow.foundation_cash_flow, 0) as foundation_cash_flow, --20% of fees supports Ethena's reserve fund
    coalesce(ena_cashflow.service_cash_flow, 0) as service_cash_flow, --80% of fees supports Ethena's ecosystem fund
    coalesce(ena_cashflow.service_cash_flow, 0) as susde_fees, 
    tvl.stablecoin_total_supply as tvl,
    tvl.stablecoin_total_supply as usde_supply,
    tvl.stablecoin_total_supply - lag(tvl.stablecoin_total_supply) over (order by date) as net_usde_supply_change,
    {{ daily_pct_change('tvl.stablecoin_total_supply') }} as tvl_growth, 
    supply_data.circulating_supply_native as circulating_supply_native,
    supply_data.circulating_supply_native - lag(supply_data.circulating_supply_native) over (order by date) as net_supply_change_native,
from usde_metrics
left join ena_metrics using(date)
left join ena_cashflow using(date)
left join tvl using(date)
left join supply_data using(date)
where usde_metrics.date < to_date(sysdate())
order by 1 desc