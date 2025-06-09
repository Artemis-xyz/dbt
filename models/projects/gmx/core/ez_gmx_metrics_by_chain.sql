{{
    config(
        materialized="table",
        snowflake_warehouse="GMX",
        database="gmx",
        schema="core",
        alias="ez_metrics_by_chain",
    )
}}

with
    trading_volume_data_v1 as (
        select date, chain, trading_volume, unique_traders
        from {{ ref("fact_gmx_trading_volume") }}
        left join {{ ref("fact_gmx_unique_traders") }} using(date)
        where chain is not null
    ),
    v2_data as (
        select date, chain, trading_volume, unique_traders
        from {{ ref("fact_gmx_v2_trading_volume_unique_traders") }}
        where chain is not null
    ),
    combined_data as (
        select 
            date,
            chain,
            sum(trading_volume) as trading_volume,
            sum(unique_traders) as unique_traders
        from (
            select * from trading_volume_data_v1
            union all
            select * from v2_data
        )
        group by 1,2
    )
    , fees_data as (
        select date, fees, revenue, supply_side_revenue
        from {{ ref("fact_gmx_all_versions_fees") }}
    )
    , spot_data as (
        select
            date,
            chain,
            sum(spot_volume) as spot_volume,
            sum(spot_fees) as spot_fees,
            sum(spot_lp_cash_flow) as spot_lp_cash_flow,
            sum(spot_stakers_cash_flow) as spot_stakers_cash_flow,
            sum(spot_oracle_cash_flow) as spot_oracle_cash_flow,
            sum(spot_treasury_cash_flow) as spot_treasury_cash_flow
        from {{ ref("fact_gmx_all_versions_dex_cash_flows") }}
        group by 1, 2
    )
    , perp_data as (
        select
            date,
            chain,
            sum(perp_volume) as perp_volume,
            sum(perp_trading_fees) as perp_trading_fees,
            sum(perp_liquidation_fees) as perp_liquidation_fees,
            sum(perp_fees) as perp_fees,
            sum(perp_lp_cash_flow) as perp_lp_cash_flow,
            sum(perp_stakers_cash_flow) as perp_stakers_cash_flow,
            sum(perp_oracle_cash_flow) as perp_oracle_cash_flow,
            sum(perp_treasury_cash_flow) as perp_treasury_cash_flow
        from {{ ref("fact_gmx_all_versions_perp_cash_flows") }}
        group by 1, 2
    )
    ,tvl_metrics as (
        select
            date,
            chain,
            sum(tvl_token_adjusted) as tvl
        from {{ ref("fact_gmx_all_versions_tvl") }}
        where version = 'v2'
        group by 1, 2

        union all   

        select
            date,
            chain,
            sum(tvl_token_adjusted) as tvl
        from {{ ref("fact_gmx_all_versions_tvl") }}
        where version = 'v1'
        group by 1, 2
    ),
    tvl_metrics_grouped as (
        select
            date,
            chain,
            sum(tvl) as tvl
        from tvl_metrics
        group by 1, 2
    ),
    token_incentives as (
        select
            claim_date as date,
            chain,
            MAX(token_incentive_usd) as token_incentives
        from {{ref('fact_gmx_token_incentives')}}
        group by 1, 2
    ),
    date_spine as (
        select date, chain
        from {{ ref("dim_date_spine") }}
        cross join (select distinct chain from tvl_metrics )
        where date between '2020-03-01' and to_date(sysdate())
    )

select 
    date_spine.date as date
    , date_spine.chain
    , 'gmx' as app
    , 'DeFi' as category

    --Old Metrics needed for backward compatibility
    , coalesce(combined_data.trading_volume, 0) as trading_volume
    , coalesce(combined_data.unique_traders, 0) as unique_traders

    --Standardized Metrics
    , coalesce(spot_data.spot_fees, 0) as spot_fees
    , coalesce(perp_data.perp_liquidation_fees, 0) as perp_liquidation_fees
    , coalesce(perp_data.perp_trading_fees, 0) as perp_trading_fees
    , coalesce(perp_data.perp_fees, 0) as perp_fees
    , coalesce(spot_data.spot_fees, 0) + coalesce(perp_data.perp_fees, 0) as ecosystem_revenue
    , coalesce(spot_data.spot_lp_cash_flow, 0) + coalesce(perp_data.perp_lp_cash_flow, 0) as service_cash_flow
    , coalesce(spot_data.spot_stakers_cash_flow, 0) + coalesce(perp_data.perp_stakers_cash_flow, 0) as staking_cash_flow
    , coalesce(spot_data.spot_oracle_cash_flow, 0) + coalesce(perp_data.perp_oracle_cash_flow, 0) as other_cash_flow
    , coalesce(spot_data.spot_treasury_cash_flow, 0) + coalesce(perp_data.perp_treasury_cash_flow, 0) as treasury_cash_flow
    , coalesce(spot_data.spot_volume, 0) as spot_volume
    , coalesce(perp_data.perp_volume, 0) as perp_volume
    , coalesce(token_incentives.token_incentives, 0) as token_incentives
    , coalesce(tvl_metrics_grouped.tvl, 0) as tvl

from date_spine
left join tvl_metrics_grouped using(date, chain)
left join token_incentives using(date, chain)
left join spot_data using(date, chain)
left join perp_data using(date, chain)
left join combined_data using(date, chain)
where date < to_date(sysdate())