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
        sum(collateral_fees.collateral_fee) as collateral_fee,
        sum(yield_fees.fees) as yield_fees,
    FROM  {{ ref('fact_ethena_yield_fees') }} yield_fees
    left join  {{ ref('fact_ethena_collateral_fees') }} collateral_fees using(date)
    group by 1
)
, ena_cashflow as (
    SELECT
        date,
        service_fee_allocation,
        foundation_fee_allocation
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
, market_data as (
    {{ get_coingecko_metrics('ethena') }}
)
select
    usde_metrics.date
    'ethena' as artemis_id

    -- Standardized Metrics

    -- Market Data
    , market_data.price as price
    , market_data.market_cap as market_cap
    , market_data.fdmc as fdmc
    , market_data.token_volume as token_volume

    -- Usage Data
    , usde_metrics.stablecoin_dau as stablecoin_dau
    , usde_metrics.stablecoin_dau as dau
    , usde_metrics.stablecoin_txns as stablecoin_txns
    , usde_metrics.stablecoin_txns as txns
    , tvl.stablecoin_total_supply as stablecoin_tvl 
    , tvl.stablecoin_total_supply as tvl

    -- Fee Data
    , ena_metrics.collateral_fee as collateral_fee
    , ena_metrics.yield_fees as yield_fees
    , coalesce(ena_metrics.collateral_fee, 0) + coalesce(ena_metrics.yield_fees, 0) as fees
    , ena_cashflow.foundation_fee_allocation as foundation_fee_allocation
    , ena_cashflow.service_fee_allocation as service_fee_allocation

    -- Financial Statements
    , 0 as revenue

    -- Stablecoin Data 
    , tvl.stablecoin_total_supply as stablecoin_total_supply

    -- Turnover Data
    , market_data.token_turnover_circulating as token_turnover_circulating
    , market_data.token_turnover_fdv as token_turnover_fdv
from usde_metrics
left join ena_metrics using(date)
left join ena_cashflow using(date)
left join tvl using(date)
left join market_data using(date)
where usde_metrics.date < to_date(sysdate())
order by 1 desc