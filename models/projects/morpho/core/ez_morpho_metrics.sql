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
        full_refresh=var("full_refresh", false),
        tags=["ez_metrics"],
    )
 }}

{% set backfill_date = var("backfill_date", None) %}

with morpho_data as (
    select
        date
        , sum(coalesce(dau, 0)) as dau
        , sum(coalesce(txns, 0)) as txns
        , sum(coalesce(borrow_amount_usd, 0)) as borrow_amount_usd
        , sum(coalesce(supply_amount_usd, 0)) as supply_amount_usd
        , sum(coalesce(supply_amount_usd, 0)) + sum(coalesce(collat_amount_usd, 0)) as deposit_amount_usd
        , sum(coalesce(fees_usd, 0)) as fees
    from {{ ref("fact_morpho_data") }}
    group by 1
)

, all_token_incentives as (
    select date, chain, coalesce(amount_native, 0) as amount_native, coalesce(amount_usd, 0) as amount_usd from {{ ref('fact_morpho_base_token_incentives') }}
    union all
    select date, chain, coalesce(amount_native, 0) as amount_native, coalesce(amount_usd, 0) as amount_usd from {{ ref('fact_morpho_ethereum_token_incentives') }}
)

, morpho_token_incentives as (
    select
        date
        , sum(coalesce(amount_native, 0)) as token_incentives_native
        , sum(coalesce(amount_usd, 0)) as token_incentives
    from all_token_incentives
    group by 1
)

, morpho_fundamental_metrics as (
    select
        d.date
        , coalesce(d.dau, 0) as dau
        , coalesce(d.txns, 0) as txns
        , sum(coalesce(d.borrow_amount_usd, 0)) over (order by d.date rows between unbounded preceding and current row) as borrows
        , sum(coalesce(d.supply_amount_usd, 0)) over (order by d.date rows between unbounded preceding and current row) as supplies
        , sum(coalesce(d.deposit_amount_usd, 0)) over (order by d.date rows between unbounded preceding and current row) as deposits
        , coalesce(d.fees, 0) as fees
        , sum(coalesce(d.fees, 0)) over (order by d.date rows between unbounded preceding and current row) as fees_cumulative
        , deposits - borrows as tvl
    from morpho_data d
 )

, market_metrics as (
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
    date_spine.date
    , 'morpho' as artemis_id
    
    -- Standardized Metrics

    -- Market Data
    , market_metrics.price
    , market_metrics.market_cap
    , market_metrics.fdmc
    , market_metrics.token_volume

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

    -- Token Turnover/Other Data
    , market_metrics.token_turnover_circulating
    , market_metrics.token_turnover_fdv

    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on

from date_spine
left join morpho_fundamental_metrics using (date)
left join morpho_supply_data using (date)
left join morpho_token_incentives using (date)
left join market_metrics using (date)
where true
{{ ez_metrics_incremental('date_spine.date', backfill_date) }}
and date_spine.date < to_date(sysdate())