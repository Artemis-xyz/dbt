{{
    config(
        materialized="table",
        snowflake_warehouse="JUPITER",
        database="jupiter",
        schema="core",
        alias="ez_metrics",
    )
}}

with all_trade_metrics as (
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
    where date < to_date(sysdate())
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
    where date between (select min(date) from all_trade_metrics) and (to_date(sysdate()))
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
, other_revenue AS (
    select
        s.date,
        s.total_jup_studio_revenue_usd as studio_revenue,
        s.total_fees                   as studio_fees,
        a.usd_revenue                  as ape_revenue,
        st.usd_revenue                 as staking_revenue
    from {{ ref("fact_jupiter_studio_fee") }} s
    left join {{ ref("fact_jupiter_staking_fee") }} st
        on s.date = st.date
    left join {{ ref("fact_jupiter_ape_pro_fee") }} a
        on s.date = a.date
), supply_data AS (
    SELECT
        date,
        burn,
        cumulative_burn,
        buyback,
        max_supply,
        total_supply,
        issued_supply,
        circulating_supply
    FROM {{ ref('fact_jupiter_supply') }}
)

select
    date_spine.date
    , 'solana' as chain
    , 'jupiter' as protocol

    -- Old metrics needed for compatibility
    -- Fees
    , (all_trade_metrics.fees
        + coalesce(other_revenue.studio_fees,0)
        + coalesce(other_revenue.ape_revenue,0)
        + coalesce(other_revenue.staking_revenue,0)
      ) as total_fees


    -- Revenue
    , all_trade_metrics.perp_revenue
    , all_trade_metrics.aggregator_revenue
    , (all_trade_metrics.revenue
       + coalesce(other_revenue.studio_revenue,0)
       + coalesce(other_revenue.ape_revenue,0) -- Fees = Revenue for ape pro
       + coalesce(other_revenue.staking_revenue,0) -- Fees = Revenue for staking
      ) as protocol_revenue

    -- Volume
    , all_trade_metrics.trading_volume

    -- DAU
    , all_trade_metrics.unique_traders -- perps specific metric
    , all_trade_metrics.aggregator_unique_traders -- aggregator specific metric
    , all_trade_metrics.txns

    -- Standardized Metrics

    --Market Metrics
    , market_metrics.price
    , market_metrics.market_cap
    , market_metrics.fdmc
    , market_metrics.token_volume

    -- Usage Metrics
    , aggregator_volume_data.single as aggregator_volume_single
    , aggregator_volume_data.overall as aggregator_volume_overall
    , all_trade_metrics.aggregator_volume as aggregator_volume
    , all_trade_metrics.trading_volume as perp_volume
    , all_trade_metrics.dca_volume
    , all_trade_metrics.limit_order_volume
    , all_trade_metrics.aggregator_txns
    , all_trade_metrics.perp_txns
    , all_trade_metrics.dca_txns
    , all_trade_metrics.limit_order_txns
    , all_trade_metrics.unique_traders as perp_dau -- perps specific metric
    , all_trade_metrics.aggregator_unique_traders as aggregator_dau -- aggregator specific metric
    , aggregator_dau + perp_dau as dau -- necessary for OL index pipeline
    , tvl.perp_tvl
    , tvl.lst_tvl
    , tvl.tvl

    -- Cashflow Metrics
    , all_trade_metrics.perp_fees
    , all_trade_metrics.aggregator_fees
    , all_trade_metrics.dca_fees
    , all_trade_metrics.limit_order_fees
    , all_trade_metrics.fees as ecosystem_revenue
    , all_trade_metrics.aggregator_fees - all_trade_metrics.aggregator_revenue as integrator_fee_allocation
    , perp_supply_side_revenue as service_fee_allocation
    , all_trade_metrics.revenue as treasury_fee_allocation
    , all_trade_metrics.perp_revenue as perp_treasury_fee_allocation
    , all_trade_metrics.aggregator_revenue as aggregator_treasury_fee_allocation
    , all_trade_metrics.dca_revenue as dca_treasury_fee_allocation
    , all_trade_metrics.limit_order_revenue as limit_order_treasury_fee_allocation
    , all_trade_metrics.buyback as buyback_fee_allocation

    -- Token Turnover Metrics
    , market_metrics.token_turnover_circulating
    , market_metrics.token_turnover_fdv

    -- JUP Token Supply Data
    , coalesce(daily_supply_data.emissions_native, 0) as emissions_native
    , coalesce(daily_supply_data.premine_unlocks_native, 0) as premine_unlocks_native
    , coalesce(daily_supply_data.emissions_native, 0) + coalesce(daily_supply_data.premine_unlocks_native, 0) - coalesce(daily_supply_data.burns_native, 0) as net_supply_change_native

    -- New Revenue Metrics
    , other_revenue.studio_revenue as launchpad_revenue
    , other_revenue.studio_fees as launchpad_fee
    , other_revenue.ape_revenue as spot_revenue
    , other_revenue.staking_revenue as lst_revenue

    -- Supply Data
    , supply_data.burn as burn_native
    , supply_data.cumulative_burn as total_burn_native
    , supply_data.buyback as total_buybacks_native
    , supply_data.max_supply 
    , supply_data.total_supply
    , supply_data.issued_supply
    , supply_data.circulating_supply

FROM date_spine
left join market_metrics using (date)
left join aggregator_volume_data using (date)
left join all_trade_metrics using (date)
left join daily_supply_data using (date)
left join tvl using (date)
left join solana_price sp using (date)
left join other_revenue using (date)
left join supply_data using (date)
