{{
    config(
        materialized="table",
        snowflake_warehouse = 'MORPHO',
        database = 'MORPHO',
        schema = 'core',
        alias = 'ez_metrics_by_chain'
    )
}}

with deposits as (
    select
        date
        , dau
        , txns
        , borrow_amount_usd
        , supply_amount_usd
        , supply_amount_usd + collat_amount_usd as deposit_amount_usd
        , fees_usd
        , chain
    from {{ ref("fact_morpho_data") }}
)

, cumulative_metrics as (
    select
        d.date
        , d.dau
        , d.txns
        , sum(d.borrow_amount_usd) over (partition by d.chain order by d.date rows between unbounded preceding and current row) as borrows
        , sum(d.supply_amount_usd) over (partition by d.chain order by d.date rows between unbounded preceding and current row) as supplies
        , sum(d.deposit_amount_usd) over (partition by d.chain order by d.date rows between unbounded preceding and current row) as deposits
        , d.fees_usd as fees
        , sum(d.fees_usd) over (partition by d.chain order by d.date rows between unbounded preceding and current row) as fees_cumulative
        , sum(d.deposit_amount_usd - d.borrow_amount_usd) over (partition by d.chain order by d.date rows between unbounded preceding and current row) as tvl
        , d.chain
    from deposits d
)

, all_token_incentives as (
    select date, chain, amount_native, amount_usd from {{ ref('fact_morpho_base_token_incentives') }}
    union all
    select date, chain, amount_native, amount_usd from {{ ref('fact_morpho_ethereum_token_incentives') }}
)

, morpho_token_incentives as (
    select
        date
        , chain
        , sum(amount_native) as token_incentives_native
        , sum(amount_usd) as token_incentives
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
    ds.date
    , coalesce(mti.chain, cm.chain) as chain
    , dau
    , txns
    , borrows
    , supplies as total_available_supply
    , deposits

    -- Standardized metrics
    , borrows as lending_loans
    , supplies as lending_loan_capacity
    , deposits as lending_deposits
    , tvl
    
    -- Cash Flow Metrics (Interest goes to Liquidity Suppliers (Lenders) + Vaults Performance Fees)
    , fees as lending_interest_fees
    , fees as fees

    , token_incentives_native
    , token_incentives
    
from date_spine ds 
left join morpho_token_incentives mti on ds.date = mti.date 
left join cumulative_metrics cm on mti.date = cm.date
                                    and mti.chain = cm.chain