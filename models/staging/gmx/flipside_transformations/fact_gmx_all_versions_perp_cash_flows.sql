{{ config(materialized="table", snowflake_warehouse="GMX") }}

with perp_trades as ( 
    select
        date,
        chain,
        version,
        sum(fees) as perp_trading_fees,
        sum(volume) as perp_volume,
        count(distinct tx_hash) as perp_txns,
        count(distinct trader) as perp_traders
    from {{ref('fact_gmx_all_versions_perp_trades')}}
    where version = 'v2'
    group by 1,2,3

    union all 

    select
        date,
        chain,
        version,
        sum(fees) as perp_trading_fees,
        sum(volume) as perp_volume,
        count(distinct tx_hash) as perp_txns,
        count(distinct trader) as perp_traders
    from {{ref('fact_gmx_all_versions_perp_trades')}}
    where version = 'v1'
    group by 1,2,3
), liquidation_fees as (
    select
        date,
        chain,
        version,
        sum(fees) as perp_liquidation_fees
    from {{ref('fact_gmx_v1_perp_liquidation_fees')}}
    group by 1,2,3

    union all

    select
        date,
        chain,
        version,
        sum(fees) as perp_liquidation_fees
    from {{ref('fact_gmx_v2_perp_liquidation_fees')}}
    group by 1,2,3
)
select 
    perp_trades.date,
    perp_trades.chain,
    perp_trades.version,
    perp_trades.perp_txns,
    perp_trades.perp_traders,
    perp_trades.perp_volume,
    perp_trades.perp_trading_fees,
    coalesce(liquidation_fees.perp_liquidation_fees, 0) as perp_liquidation_fees,
    coalesce(liquidation_fees.perp_liquidation_fees, 0) + coalesce(perp_trades.perp_trading_fees, 0) as perp_fees,
    CASE
        WHEN version = 'v1' THEN 0.7 * perp_fees
        WHEN version = 'v2' THEN 0.63 * perp_fees
    END as perp_lp_fee_allocation,
    CASE
        WHEN version = 'v1' THEN 0.3 * perp_fees
        WHEN version = 'v2' THEN 0.27 * perp_fees
    END as perp_stakers_fee_allocation,
    CASE
        WHEN version = 'v1' THEN 0 * perp_fees
        WHEN version = 'v2' THEN 0.012 * perp_fees
    END as perp_oracle_fee_allocation,
    CASE
        WHEN version = 'v1' THEN 0 * perp_fees
        WHEN version = 'v2' THEN 0.088 * perp_fees
    END as perp_treasury_fee_allocation
from perp_trades
left join liquidation_fees using (date, chain, version)