{{
    config(
        materialized="table",
        snowflake_warehouse = 'MORPHO',
        database = 'MORPHO',
        schema = 'core',
        alias = 'ez_metrics_by_chain'
    )
}}

with morpho_data as (
    select
        date
        , coalesce(dau, 0) as dau
        , coalesce(txns, 0) as txns
        , coalesce(borrow_amount_usd, 0) as borrow_amount_usd
        , coalesce(supply_amount_usd, 0) as supply_amount_usd
        , coalesce(supply_amount_usd, 0) + coalesce(collat_amount_usd, 0) as deposit_amount_usd
        , coalesce(fees_usd, 0) as fees_usd
        , chain
    from {{ ref("fact_morpho_data") }}
)

, morpho_fundamental_metrics as (
    select
        date
        , coalesce(dau, 0) as dau
        , coalesce(txns, 0) as txns
        , sum(coalesce(borrow_amount_usd, 0)) over (partition by chain order by date rows between unbounded preceding and current row) as borrows
        , sum(coalesce(supply_amount_usd, 0)) over (partition by chain order by date rows between unbounded preceding and current row) as supplies
        , sum(coalesce(deposit_amount_usd, 0)) over (partition by chain order by date rows between unbounded preceding and current row) as deposits
        , coalesce(fees_usd, 0) as fees
        , sum(coalesce(fees_usd, 0)) over (partition by chain order by date rows between unbounded preceding and current row) as fees_cumulative
        , sum(coalesce(deposit_amount_usd, 0) - coalesce(borrow_amount_usd, 0)) over (partition by chain order by date rows between unbounded preceding and current row) as tvl
        , d.chain
    from morpho_data
)

, all_token_incentives as (
    select date, chain, coalesce(amount_native, 0) as amount_native, coalesce(amount_usd, 0) as amount_usd from {{ ref('fact_morpho_base_token_incentives') }}
    union all
    select date, chain, coalesce(amount_native, 0) as amount_native, coalesce(amount_usd, 0) as amount_usd from {{ ref('fact_morpho_ethereum_token_incentives') }}
)

, morpho_token_incentives as (
    select
        date
        , chain
        , sum(coalesce(amount_native, 0)) as token_incentives_native
        , sum(coalesce(amount_usd, 0)) as token_incentives
    from all_token_incentives
    group by 1, 2
)

, date_spine as (
    select
        date
    from {{ ref("dim_date_spine") }}
    where date < to_date(sysdate()) and date >= (select min(date) from morpho_token_incentives)
)

select
    date_spine.date
    , 'morpho' as artemis_id
    , morpho_fundamental_metrics.chain

    -- Standardized Metrics

    -- Usage Data
    , morpho_fundamental_metrics.dau as lending_dau
    , morpho_fundamental_metrics.txns as lending_txns
    , morpho_fundamental_metrics.borrows as lending_loans
    , morpho_fundamental_metrics.supplies as lending_loan_capacity
    , morpho_fundamental_metrics.deposits as lending_deposits
    , morpho_fundamental_metrics.tvl
    
    -- Financial Statements (Interest goes to Liquidity Suppliers (Lenders) + Vaults Performance Fees)
    , morpho_fundamental_metrics.fees as lending_interest_fees
    , 0 as revenue
    , morpho_token_incentives.token_incentives_native
    , morpho_token_incentives.token_incentives
    , revenue - morpho_token_incentives.token_incentives as earnings
    
    -- Supply Data
    , morpho_supply_data.premine_unlocks_native
    , morpho_supply_data.circulating_supply_native
    
from date_spine 
left join morpho_token_incentives using (date, chain)
left join morpho_fundamental_metrics using (date, chain)