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
        , fees_usd
        , chain
    from {{ ref("fact_morpho_data") }}
),
cumulative_metrics as (
    select
        d.date
        , sum(d.borrow_amount_usd) over (partition by d.chain order by d.date rows between unbounded preceding and current row) as borrows
        , sum(d.supply_amount_usd) over (partition by d.chain order by d.date rows between unbounded preceding and current row) as supplies
        , sum(d.deposit_amount_usd) over (partition by d.chain order by d.date rows between unbounded preceding and current row) as deposits
        , d.fees_usd as fees
        , sum(d.fees_usd) over (partition by d.chain order by d.date rows between unbounded preceding and current row) as fees_cumulative
        , sum(d.deposit_amount_usd - d.borrow_amount_usd) over (partition by d.chain order by d.date rows between unbounded preceding and current row) as tvl
        , d.chain
    from deposits d
)
select
    date
    , borrows as daily_borrows_usd
    , supplies as total_available_supply
    , deposits as daily_supply_usd
    , fees
    , chain

    -- Standardized metrics
    , tvl
    , fees as gross_protocol_revenue
    , borrows as lending_borrows
    , supplies as lending_total_available_borrows
    , deposits as lending_deposits
from cumulative_metrics 