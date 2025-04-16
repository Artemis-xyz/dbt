{{
    config(
        materialized="table",
        snowflake_warehouse="GMX",
        database="gmx",
        schema="core",
        alias="ez_metrics",
    )
}}

with
    --Old data needed for backward compatibility
    trading_volume_data_v1 as (
        select date, trading_volume, unique_traders
        from {{ ref("fact_gmx_trading_volume") }}
        left join {{ ref("fact_gmx_unique_traders") }} using(date)
        where chain is not null
    ),
    v2_data as (
        select date, trading_volume, unique_traders
        from {{ ref("fact_gmx_v2_trading_volume_unique_traders") }}
        where chain is not null
    ),
    combined_data as (
        select 
            date,
            sum(trading_volume) as trading_volume,
            sum(unique_traders) as unique_traders
        from (
            select * from trading_volume_data_v1
            union all
            select * from v2_data
        )
        group by 1
    )
    --Current data
    , spot_data as (
        select
            date,
            sum(spot_volume) as spot_volume,
            sum(spot_fees) as spot_fees,
            sum(spot_lp_cash_flow) as spot_lp_cash_flow,
            sum(spot_stakers_cash_flow) as spot_stakers_cash_flow,
            sum(spot_oracle_cash_flow) as spot_oracle_cash_flow,
            sum(spot_treasury_cash_flow) as spot_treasury_cash_flow
        from {{ ref("fact_gmx_all_versions_dex_cash_flows") }}
        group by 1
    )
    , perp_data as (
        select
            date,
            sum(perp_volume) as perp_volume,
            sum(perp_trading_fees) as perp_trading_fees,
            sum(perp_liquidation_fees) as perp_liquidation_fees,
            sum(perp_fees) as perp_fees,
            sum(perp_lp_cash_flow) as perp_lp_cash_flow,
            sum(perp_stakers_cash_flow) as perp_stakers_cash_flow,
            sum(perp_oracle_cash_flow) as perp_oracle_cash_flow,
            sum(perp_treasury_cash_flow) as perp_treasury_cash_flow
        from {{ ref("fact_gmx_all_versions_perp_cash_flows") }}
        group by 1
    )
    , txns_and_dau_data as (
        select
            date,
            gmx_dau,
            spot_dau,
            perp_dau,
            gmx_txns,
            spot_txns,
            perp_txns
        from {{ ref("fact_gmx_all_versions_txns_and_dau") }}
    )
    , treasury_metrics as (
        select * from {{ ref("fact_gmx_treasury_data") }}
    ),
    tvl_metrics as (
        select
            date,
            sum(tvl_token_adjusted) as tvl
        from {{ ref("fact_gmx_all_versions_tvl") }}
        where version = 'v2'
        group by 1

        union all   

        select
            date,
            sum(tvl_token_adjusted) as tvl
        from {{ ref("fact_gmx_all_versions_tvl") }}
        where version = 'v1'
        group by 1
    ),
    tvl_metrics_grouped as (
        select
            date,
            sum(tvl) as tvl
        from tvl_metrics
        group by 1
    ),
    date_spine as (
        select date
        from {{ ref("dim_date_spine") }}
        where date between '2021-08-29' and to_date(sysdate())
    ),
    treasury as (
        select
            date,
            sum(usd_balance) as net_treasury_usd
        from {{ ref('fact_gmx_treasury_data') }}
        group by 1
    ), treasury_native as (
        select
            date,
            sum(native_balance) as treasury_native
        from {{ ref('fact_gmx_treasury_data') }}
        where token = 'GMX'
        group by 1
    ), net_treasury as (
        select
            date,
            sum(usd_balance) as net_treasury_usd
        from {{ ref('fact_gmx_treasury_data') }}
        where token <> 'GMX'
        group by 1
    )
    ,fees_data as (
        select date, fees, revenue, supply_side_revenue
        from {{ ref("fact_gmx_all_versions_fees") }}
    )
    ,market_metrics as ({{ get_coingecko_metrics("gmx") }})

select 
    date_spine.date as date
    , 'gmx' as app
    , 'DeFi' as category

    --Old Metrics needed for backward compatibility
    , coalesce(combined_data.trading_volume, 0) as trading_volume
    , coalesce(combined_data.unique_traders, 0) as unique_traders
    , coalesce(fees_data.fees, 0) as fees
    , coalesce(fees_data.revenue, 0) as revenue
    , coalesce(treasury_native.treasury_native, 0) as treasury_value_native    


    --Standardized Metrics
    , coalesce(spot_data.spot_fees, 0) as spot_fees
    , coalesce(perp_data.perp_liquidation_fees, 0) as perp_liquidation_fees
    , coalesce(perp_data.perp_trading_fees, 0) as perp_trading_fees
    , coalesce(perp_data.perp_fees, 0) as perp_fees
    , coalesce(spot_data.spot_fees, 0) + coalesce(perp_data.perp_fees, 0) as gross_protocol_revenue
    , coalesce(spot_data.spot_lp_cash_flow, 0) + coalesce(perp_data.perp_lp_cash_flow, 0) as lp_cash_flow
    , coalesce(spot_data.spot_stakers_cash_flow, 0) + coalesce(perp_data.perp_stakers_cash_flow, 0) as stakers_cash_flow
    , coalesce(spot_data.spot_oracle_cash_flow, 0) + coalesce(perp_data.perp_oracle_cash_flow, 0) as oracle_cash_flow
    , coalesce(spot_data.spot_treasury_cash_flow, 0) + coalesce(perp_data.perp_treasury_cash_flow, 0) as treasury_cash_flow

    , coalesce(spot_data.spot_volume, 0) as spot_volume
    , coalesce(perp_data.perp_volume, 0) as perp_volume

    , coalesce(txns_and_dau_data.spot_txns, 0) as spot_txns
    , coalesce(txns_and_dau_data.perp_txns, 0) as perp_txns
    , coalesce(txns_and_dau_data.gmx_txns, 0) as gmx_txns
    , coalesce(txns_and_dau_data.spot_dau, 0) as spot_dau
    , coalesce(txns_and_dau_data.perp_dau, 0) as perp_dau
    , coalesce(txns_and_dau_data.gmx_dau, 0) as gmx_dau
    
    , coalesce(tvl_metrics_grouped.tvl, 0) as tvl
    , coalesce(treasury.net_treasury_usd, 0) as treasury_value
    , coalesce(net_treasury.net_treasury_usd, 0) as net_treasury_value
    , coalesce(treasury_native.treasury_native, 0) as own_token_treasury    
    
    -- Market Data
    , market_metrics.price as price
    , market_metrics.market_cap as market_cap
    , market_metrics.fdmc as fdmc
    , market_metrics.token_turnover_circulating as token_turnover_circulating
    , market_metrics.token_turnover_fdv as token_turnover_fdv
    , market_metrics.token_volume as token_volume
from date_spine
left join tvl_metrics_grouped using(date)
left join perp_data using(date)
left join spot_data using(date)
left join txns_and_dau_data using(date)
left join treasury using(date)
left join treasury_native using(date)
left join net_treasury using(date)
left join combined_data using(date)
left join fees_data using(date)
left join market_metrics using(date)
where date < to_date(sysdate())