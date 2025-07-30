{{
    config(
        materialized="incremental",
        snowflake_warehouse="marinade",
        database="marinade",
        schema="core",
        alias="ez_metrics",
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

with marinade_tvl as (
    select
        date
        , coalesce(liquid, 0) as liquid
        , coalesce(native, 0) as native
        , coalesce(tvl, 0) as tvl
    from {{ ref("fact_marinade_tvl") }}
)
, marinade_dau_txns as (
    select
        date
        , coalesce(dau, 0) as dau
        , coalesce(txns, 0) as txns
    from {{ ref("fact_marinade_dau_txns") }}
)
, marinade_v1_fees as (
    select
        date
        , coalesce(unstaking_fees, 0) as unstaking_fees_native
        , coalesce(fees_native, 0) as fees_native
    from {{ ref("fact_marinade_v1_fees") }}
)
, marinade_v2_fees as (
    select
        date
        , coalesce(fees_native, 0) as fees_native
    from {{ ref("fact_marinade_fees") }}
)
, marinade_fees as (
    select
        coalesce(v1_fees.date, v2_fees.date) as date
        , coalesce(v1_fees.unstaking_fees_native, 0) as unstaking_fees_native
        , coalesce(v1_fees.fees_native, 0) + coalesce(v2_fees.fees_native, 0) as fees_native
    from v1_fees
    full outer join v2_fees using (date)
)
, marinade_circulating_supply as (
    select
        date
        , coalesce(circulating_supply, 0) as circulating_supply
    from {{ ref("fact_marinade_circulating_supply") }}
)
, solana_price as (
    select * from ({{ get_coingecko_price_with_latest("solana") }}) 
)
, market_metrics as (
    {{ get_coingecko_metrics("marinade") }}
)

select
    marinade_tvl.date
    , 'marinade' as artemis_id
    
    -- Standardized Metrics
    
    -- Market Data
    , market_metrics.price
    , market_metrics.market_cap
    , market_metrics.fdmc
    , market_metrics.token_volume

    -- Usage Data
    , marinade_dau_txns.dau as lst_dau
    , marinade_dau_txns.dau as dau
    , marinade_dau_txns.txns as lst_txns
    , marinade_dau_txns.txns as txns
    , marinade_tvl.tvl * solana_price.price as lst_tvl
    , marinade_tvl.tvl * solana_price.price as tvl
    , marinade_tvl.tvl - lag(marinade_tvl.tvl) over (order by marinade_tvl.date) as lst_tvl_net_change    
    , marinade_tvl.liquid * solana_price.price as tvl_liquid_stake
    , marinade_tvl.native * solana_price.price as tvl_native_stake

    -- Fee Data
    , marinade_fees.unstaking_fees_native * solana_price.price as unstaking_fees
    , marinade_fees.fees_native * solana_price.price as lst_fees
    , marinade_fees.unstaking_fees_native * solana_price.price + marinade_fees.fees_native * solana_price.price as fees
    , case when 
        marinade_tvl.date < '2024-08-18' then marinade_fees.unstaking_fees_native * solana_price.price * 0.25 + marinade_fees.fees_native * solana_price.price -- v1 - 25% goes to treasury (Fees to Holders)
        else marinade_fees.fees_native * solana_price.price -- v2 fees (2024-08-18 and after) - validator bids
    end as treasury_fee_allocation
    , case when 
        marinade_tvl.date < '2024-08-18' then marinade_fees.unstaking_fees_native * solana_price.price * 0.75  -- v1 - unstaking fees 75% goes to LP (Fees to Suppliers) 
        else 0  -- when v2 fees are active, none goes to LP (Fees to Suppliers)
    end as service_fee_allocation

    -- Financial Statements
    , treasury_fee_allocation as revenue

    -- Supply Metrics
    , marinade_circulating_supply.circulating_supply as circulating_supply_native
    
    -- Token Turnover/Other Data
    , market_metrics.token_turnover_circulating
    , market_metrics.token_turnover_fdv

    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on

from marinade_tvl
left join marinade_dau_txns using (date)
left join marinade_fees using (date)
left join marinade_circulating_supply using (date)
left join price using (date)
left join market_metrics using (date)
where true
{{ ez_metrics_incremental('marinade_tvl.date', backfill_date) }}
and marinade_tvl.date < to_date(sysdate())