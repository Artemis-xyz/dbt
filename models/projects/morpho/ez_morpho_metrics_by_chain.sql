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
        , borrow_amount_usd
        , supply_amount_usd
        , supply_amount_usd + collat_amount_usd as deposit_amount_usd
        , chain
    from {{ ref("fact_morpho_deposits_loans") }}
),
fees as (
    select
        date
        , interest_usd
        , chain
    from {{ ref("fact_morpho_fees") }}
),
cumulative_metrics as (
    select
        d.date,
        sum(d.borrow_amount_usd) over (partition by d.chain order by d.date rows between unbounded preceding and current row) as borrows,
        sum(d.supply_amount_usd) over (partition by d.chain order by d.date rows between unbounded preceding and current row) as supplies,
        sum(d.deposit_amount_usd) over (partition by d.chain order by d.date rows between unbounded preceding and current row) as deposits,
        sum(f.interest_usd) over (partition by d.chain order by d.date rows between unbounded preceding and current row) as fees,
        sum(d.deposit_amount_usd - d.borrow_amount_usd) over (partition by d.chain order by d.date rows between unbounded preceding and current row) as tvl,
        d.chain
    from deposits d
    left join fees f on d.date = f.date and d.chain = f.chain
)
select
    date
    , borrows
    , supplies
    , deposits
    , tvl
    , fees
    , chain
from cumulative_metrics 