{{
    config(
        materialized="incremental",
        snowflake_warehouse="JUPITER",
        database="jupiter",
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

with fundamentals as (
    select
        date,
        -- Fees
        SUM(CASE WHEN trade_type = 'perps' THEN fees ELSE 0 END) as perp_fees, -- Perps specific metric
        SUM(CASE WHEN trade_type = 'aggregator' THEN fees ELSE 0 END) as aggregator_fees, -- Aggregator specific metric
        SUM(CASE WHEN trade_type = 'dca' THEN fees ELSE 0 END) as dca_fees,
        SUM(CASE WHEN trade_type = 'limit_order' THEN fees ELSE 0 END) as limit_order_fees,
        sum(fees) as fees,
        -- Revenue
        sum(CASE WHEN trade_type = 'perps' THEN revenue ELSE 0 END) as perp_revenue, -- Perps specific metric
        sum(CASE WHEN trade_type = 'aggregator' THEN revenue ELSE 0 END) as aggregator_revenue, -- Aggregator specific metric
        sum(CASE WHEN trade_type = 'dca' THEN revenue ELSE 0 END) as dca_revenue,
        sum(CASE WHEN trade_type = 'limit_order' THEN revenue ELSE 0 END) as limit_order_revenue,
        sum(revenue) as revenue,
        sum(CASE WHEN date >= '2025-02-17' THEN revenue * 0.5 ELSE 0 END) as buyback,
        -- Supply Side Revenue
        sum(CASE WHEN trade_type = 'perps' THEN supply_side_revenue ELSE 0 END) as perp_supply_side_revenue,
        SUM(CASE WHEN trade_type = 'perps' THEN supply_side_revenue ELSE 0 END) as primary_supply_side_revenue,
        sum(supply_side_revenue) as total_supply_side_revenue,
        -- Volume
        sum(CASE WHEN trade_type = 'perps' THEN volume ELSE 0 END) as trading_volume, -- Perps specific metric
        sum(CASE WHEN trade_type = 'aggregator' THEN volume ELSE 0 END) as aggregator_volume, -- Aggregator specific metric
        sum(CASE WHEN trade_type = 'dca' THEN volume ELSE 0 END) as dca_volume,
        sum(CASE WHEN trade_type = 'limit_order' THEN volume ELSE 0 END) as limit_order_volume,
        sum(volume) as volume,
        -- Txns
        sum(CASE WHEN trade_type = 'aggregator' THEN txns ELSE 0 END) as aggregator_txns,
        sum(CASE WHEN trade_type = 'perps' THEN txns ELSE 0 END) as perp_txns,
        sum(CASE WHEN trade_type = 'dca' THEN txns ELSE 0 END) as dca_txns,
        sum(CASE WHEN trade_type = 'limit_order' THEN txns ELSE 0 END) as limit_order_txns,
        sum(txns) as txns,
        -- DAU
        sum(CASE WHEN trade_type = 'perps' THEN dau ELSE 0 END) as unique_traders, -- Perps specific metric
        sum(CASE WHEN trade_type = 'aggregator' THEN dau ELSE 0 END) as aggregator_unique_traders, -- Aggregator specific metric
        sum(dau) as dau
    from {{ ref("fact_jupiter_all_trade_metrics") }}
    group by 1
)
, aggregator_volume_data as (
select
        date,
        overall,
        single
    from {{ ref("fact_jupiter_aggregator_metrics") }}
)
, daily_supply_data as (
    select
        date,
        0 as emissions_native,
        premine_unlocks as premine_unlocks_native,
        0 as burns_native    
    from {{ ref("fact_jupiter_premine_unlocks") }}
)
, date_spine as (
    select
        date
    from {{ ref("dim_date_spine") }}
    where date between (select min(date) from fundamentals) and (to_date(sysdate()))
)
, perps_tvl as (
    select
        date,
            sum(balance) as perp_tvl
        from {{ ref("fact_jupiter_perps_tvl") }}
        where balance > 2 and balance is not null
        and contract_address not ilike '%solana%' -- Perps holds WSOL not SOL, but there's a bug in the balances table that includes both WSOL and SOL
        group by date
)
, lst_tvl as (
    select
        date,
        sum(balance) as lst_tvl,
        sum(balance_native) as lst_tvl_native
    from {{ ref("fact_jupiter_lst_tvl") }}
    where balance_native > 2 and balance_native is not null
    group by date
)
, tvl as (
    select
        date,
        perp_tvl,
        lst_tvl,
        coalesce(perp_tvl, 0) + (coalesce(lst_tvl, 0)) as tvl
    from perps_tvl
    left join lst_tvl using (date)
)
, market_metrics as ({{ get_coingecko_metrics("jupiter-exchange-solana") }}
)
, solana_price as ({{ get_coingecko_metrics("solana") }}
)
select
    date_spine.date
    , 'jupiter' as artemis_id
    -- Standardized Metrics
    --Market Metrics
    , market_metrics.price
    , market_metrics.market_cap
    , market_metrics.fdmc
    , market_metrics.token_volume
    -- Usage Metrics
    , aggregator_volume_data.single as aggregator_volume_single
    , aggregator_volume_data.overall as aggregator_volume_overall
    , f.aggregator_volume as aggregator_volume
    , f.trading_volume as perp_volume
    , f.dca_volume
    , f.limit_order_volume

    , f.aggregator_txns
    , f.perp_txns
    , f.dca_txns
    , f.limit_order_txns
    , f.txns

    , f.unique_traders as perp_dau
    , f.aggregator_unique_traders as aggregator_dau
    , aggregator_dau + perp_dau as dau

    , tvl.perp_tvl
    , tvl.lst_tvl
    , tvl.tvl

    -- Fees Metrics
    , f.perp_fees
    , f.aggregator_fees
    , f.dca_fees
    , f.limit_order_fees
    , f.fees
    , f.aggregator_fees - f.aggregator_revenue as integrator_fee_allocation
    , perp_supply_side_revenue as service_fee_allocation
    , f.revenue as treasury_fee_allocation
    , f.perp_revenue as perp_treasury_fee_allocation
    , f.aggregator_revenue as aggregator_treasury_fee_allocation
    , f.dca_revenue as dca_treasury_fee_allocation
    , f.limit_order_revenue as limit_order_treasury_fee_allocation

    -- Financial Metrics
    , f.revenue

    , f.buyback as buybacks
    -- Token Turnover Metrics
    , market_metrics.token_turnover_circulating
    , market_metrics.token_turnover_fdv
    -- JUP Token Supply Data
    , coalesce(daily_supply_data.emissions_native, 0) as emissions_native
    , coalesce(daily_supply_data.premine_unlocks_native, 0) as premine_unlocks_native
    , coalesce(daily_supply_data.burns_native, 0) as burns_native
    , coalesce(daily_supply_data.emissions_native, 0) + coalesce(daily_supply_data.premine_unlocks_native, 0) - coalesce(daily_supply_data.burns_native, 0) as net_supply_change_native
    , sum(coalesce(daily_supply_data.emissions_native, 0) + coalesce(daily_supply_data.premine_unlocks_native, 0) - coalesce(daily_supply_data.burns_native, 0)) over (order by daily_supply_data.date) as circulating_supply_native
    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on
FROM date_spine
left join market_metrics using (date)
left join aggregator_volume_data using (date)
left join fundamentals f using (date)
left join daily_supply_data using (date)
left join tvl using (date)
left join solana_price sp using (date)
where true
{{ ez_metrics_incremental('date_spine.date', backfill_date) }}
and date_spine.date < to_date(sysdate())