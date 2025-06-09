{{
    config(
        materialized="table",
        snowflake_warehouse="GMX",
        database="gmx",
        schema="core",
        alias="ez_metrics_by_version",
    )
}}

with 
    spot_data as (
        select
            date,
            version,
            sum(spot_volume) as spot_volume,
            sum(spot_fees) as spot_fees,
            sum(spot_lp_cash_flow) as spot_lp_cash_flow,
            sum(spot_stakers_cash_flow) as spot_stakers_cash_flow,
            sum(spot_oracle_cash_flow) as spot_oracle_cash_flow,
            sum(spot_treasury_cash_flow) as spot_treasury_cash_flow
        from {{ ref("fact_gmx_all_versions_dex_cash_flows") }}
        group by 1, 2
    ),
    perp_data as (
        select
            date,
            version,
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
    ),
    tvl_metrics as (
        select
            date,
            version,
            sum(tvl_token_adjusted) as tvl
        from {{ ref("fact_gmx_all_versions_tvl") }}
        where version = 'v2'
        group by 1, 2

        union all   

        select
            date,
            version,
            sum(tvl_token_adjusted) as tvl
        from {{ ref("fact_gmx_all_versions_tvl") }}
        where version = 'v1'
        group by 1, 2
    ),
    tvl_metrics_grouped as (
        select
            date,
            version,
            sum(tvl) as tvl
        from tvl_metrics
        group by 1, 2
    ),
    date_spine as (
        select date, version
        from {{ ref("dim_date_spine") }}
        cross join (select distinct version from tvl_metrics )
        where date between '2020-03-01' and to_date(sysdate())
    )

select 
    date_spine.date as date
    , date_spine.version
    , 'gmx' as app
    , 'DeFi' as category

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
    , coalesce(tvl_metrics_grouped.tvl, 0) as tvl
from date_spine
left join tvl_metrics_grouped using(date, version)
left join spot_data using(date, version)
left join perp_data using(date, version)
where date < to_date(sysdate())
