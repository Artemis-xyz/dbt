{{
    config(
        materialized="table",
        snowflake_warehouse="BEAM",
        database="beam",
        schema="core",
        alias="ez_metrics",
    )
}}

with
    fundamental_data as (
        select
            date, chain, dau, txns, fees_native
        from {{ ref("fact_beam_fundamental_metrics") }}
    )
    , price_data as ({{ get_coingecko_metrics("beam-2") }})
    , defillama_data as ({{ get_defillama_metrics("beam") }})
    , premine_unlocks as (
        select *
        from {{ ref("fact_beam_premine_unlocks") }}
    )
    , burns as (
        select *
        from {{ ref("fact_beam_burns") }}
    )
    , date_spine as (
        select date
        from {{ ref("dim_date_spine") }}
        where date < to_date(sysdate()) and date >= '2021-10-31'
    )
select
    date_spine.date as date
    , coalesce(chain, 'beam') as chain
    , coalesce(dau, 0) as dau
    , coalesce(txns, 0) as txns
    , coalesce(fees_native, 0) as fees_native
    , coalesce(fees_native, 0) * coalesce(price, 0) as fees
    , coalesce(fees, 0) / coalesce(txns, 1) as avg_txn_fee
    
    -- Standardized Metrics
    
    -- Market Metrics
    , coalesce(price, 0) as price
    , coalesce(market_cap, 0) as market_cap
    , coalesce(fdmc, 0) as fdmc
    , coalesce(token_volume, 0) as token_volume
    
    -- Chain Metrics
    , coalesce(dau, 0) as chain_dau
    , coalesce(txns, 0) as chain_txns
    
    -- Cash Flow Metrics
    , coalesce(fees, 0) as chain_fees
    , coalesce(fees, 0) as ecosystem_revenue
    , coalesce(fees_native, 0) as ecosystem_revenue_native
    , coalesce(avg_txn_fee, 0) as chain_avg_txn_fee
    , coalesce(burns.burns_native, 0) as burned_cashflow_native
    , coalesce(burns.burns_native, 0) * price as burned_cashflow
    
    -- Crypto Metrics
    , tvl
    , token_turnover_circulating
    , token_turnover_fdv

    -- Supply Metrics
    , coalesce(premine_unlocks_native, 0) as premine_unlocks_native
    , coalesce(premine_unlocks_native, 0) - coalesce(burns.burns_native, 0) as net_supply_change_native
    , sum(net_supply_change_native) over (order by date) as circulating_supply_native
from date_spine
left join fundamental_data using (date)
left join price_data using (date)
left join defillama_data using (date)
left join premine_unlocks using (date)
left join burns using (date)