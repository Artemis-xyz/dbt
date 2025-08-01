{{
    config(
        materialized="incremental",
        snowflake_warehouse = 'MORPHO',
        database = 'MORPHO',
        schema = 'core',
        alias = 'ez_metrics',
        incremental_strategy="merge",
        unique_key="date",
        on_schema_change="append_new_columns",
        merge_update_columns=var("backfill_columns", []),
        merge_exclude_columns=["created_on"] if not var("backfill_columns", []) else none,
        full_refresh=false,
        tags=["ez_metrics"],
    )
 }}

{% set backfill_date = var("backfill_date", None) %}

with morpho_data as (
    select
        date
        , sum(dau) as dau
        , sum(txns) as txns
        , sum(borrow_amount_usd) as borrow_amount_usd
        , sum(supply_amount_usd) as supply_amount_usd
        , sum(supply_amount_usd) + sum(collat_amount_usd) as deposit_amount_usd
        , sum(fees_usd) as fees
    from {{ ref("fact_morpho_data") }}
    group by 1
)

, all_token_incentives as (
    select date, chain, amount_native, amount_usd from {{ ref('fact_morpho_base_token_incentives') }}
    union all
    select date, chain, amount_native, amount_usd from {{ ref('fact_morpho_ethereum_token_incentives') }}
)

, morpho_token_incentives as (
    select
        date
        , sum(amount_native) as token_incentives_native
        , sum(amount_usd) as token_incentives
    from all_token_incentives
    group by 1
)

, cumulative_metrics as (
    select
        d.date
        , d.dau
        , d.txns
        , sum(d.borrow_amount_usd) over (order by d.date rows between unbounded preceding and current row) as borrows
        , sum(d.supply_amount_usd) over (order by d.date rows between unbounded preceding and current row) as supplies
        , sum(d.deposit_amount_usd) over (order by d.date rows between unbounded preceding and current row) as deposits
        , fees
        , sum(fees) over (order by d.date rows between unbounded preceding and current row) as fees_cumulative
        , deposits - borrows as tvl
    from morpho_data d
 )

, morpho_market_data as (
    {{ get_coingecko_metrics('morpho') }}
)

, morpho_supply_data as (
    select
        date
        , premine_unlocks_native
        , net_supply_change_native
        , circulating_supply_native
    from {{ ref("fact_morpho_supply_data") }}
)

, date_spine as (
    select
        date
    from {{ ref("dim_date_spine") }}
    where date < to_date(sysdate()) and date >= (select min(date) from morpho_token_incentives)
)

select
    date
    , dau
    , txns
    , borrows
    , supplies as total_available_supply
    , deposits
    , fees
    -- Standardized metrics
    , dau as lending_dau
    , txns as lending_txns
    , borrows as lending_loans
    , supplies as lending_loan_capacity
    , deposits as lending_deposits
    , tvl
    -- Cash Flow Metrics (Interest goes to Liquidity Suppliers (Lenders) + Vaults Performance Fees)
    , fees as lending_interest_fees
    , 0 as revenue
    , revenue - token_incentives as earnings
    -- Supply Metrics
    , msd.premine_unlocks_native
    , msd.net_supply_change_native
    , msd.circulating_supply_native
    -- Market Metrics
    , mdd.price
    , mdd.market_cap
    , mdd.fdmc
    , mdd.token_turnover_circulating
    , mdd.token_turnover_fdv
    , mdd.token_volume
    , token_incentives_native
    , token_incentives
    -- timestamp columns
    , sysdate() as created_on
    , sysdate() as modified_on
from date_spine
left join cumulative_metrics using (date)
left join morpho_market_data mdd using (date)
left join morpho_supply_data msd using (date)
left join morpho_token_incentives using (date)
where true
{{ ez_metrics_incremental('date_spine.date', backfill_date) }}
and date_spine.date < to_date(sysdate())