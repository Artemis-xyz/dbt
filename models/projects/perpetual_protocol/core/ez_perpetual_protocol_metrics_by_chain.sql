{{
    config(
        materialized="table",
        snowflake_warehouse="PERPETUAL_PROTOCOL",
        database="perpetual_protocol",
        schema="core",
        alias="ez_metrics_by_chain",
    )
}}


with
    trading_volume_data as (
        select date, trading_volume, chain
        from {{ ref("fact_perpetual_protocol_trading_volume") }}
        where chain is not null
    ),
    unique_traders_data as (
        select date, unique_traders, chain
        from {{ ref("fact_perpetual_protocol_unique_traders") }}
        where chain is not null
    ),
    fees_data as (
        select date, fees, chain
        from {{ ref("fact_perpetual_protocol_fees") }}
        where chain is not null
    ),
    tvl_data as (
        select date, tvl, chain
        from {{ ref("fact_perpetual_protocol_tvl") }}
        where chain is not null
    ), 
    token_incentives as (
        select
            date,
            chain,
            SUM(total_token_incentives) as token_incentives
        from {{ref('fact_perpetual_token_incentives')}}
        group by date, chain
    ),
    all_date_chain_combinations as (
        select distinct date, chain from trading_volume_data
        union
        select distinct date, chain from unique_traders_data  
        union
        select distinct date, chain from fees_data
        union
        select distinct date, chain from tvl_data
        union
        select distinct date, chain from token_incentives
    )

select
    base.date as date
    , 'perpetual_protocol' as app
    , 'DeFi' as category
    , base.chain
    , coalesce(tvd.trading_volume, 0) as trading_volume
    , coalesce(utd.unique_traders, 0) as unique_traders
    , coalesce(fd.fees, 0) as fees
    , coalesce(fd.fees, 0) * 0.2 as revenue -- https://support.perp.com/general/legacy-reward-programs#how-it-works search '20%'
    , (coalesce(tvl_data.tvl, 0) - LAG(coalesce(tvl_data.tvl, 0)) OVER (PARTITION BY base.chain ORDER BY base.date)) / NULLIF(LAG(coalesce(tvl_data.tvl, 0)) OVER (PARTITION BY base.chain ORDER BY base.date), 0) * 100 as tvl_growth
    -- standardize metrics
    , coalesce(tvd.trading_volume, 0) as perp_volume
    , coalesce(utd.unique_traders, 0) as perp_dau
    , coalesce(tvl_data.tvl, 0) as tvl
    , (coalesce(tvl_data.tvl, 0) - LAG(coalesce(tvl_data.tvl, 0)) OVER (PARTITION BY base.chain ORDER BY base.date)) / NULLIF(LAG(coalesce(tvl_data.tvl, 0)) OVER (PARTITION BY base.chain ORDER BY base.date), 0) * 100 as tvl_pct_change
    , coalesce(fd.fees, 0) as ecosystem_revenue
    , coalesce(fd.fees, 0) * 0.2 * 0.8 as staking_cash_flow
    , coalesce(fd.fees, 0) * 0.8 as service_cash_flow
    , coalesce(fd.fees, 0) * 0.2 * 0.2 as treasury_cash_flow
    , coalesce(ti.token_incentives, 0) as token_incentives
from all_date_chain_combinations base
left join trading_volume_data tvd on tvd.date = base.date and tvd.chain = base.chain
left join unique_traders_data utd on utd.date = base.date and utd.chain = base.chain
left join fees_data fd on fd.date = base.date and fd.chain = base.chain
left join tvl_data tvl_data on tvl_data.date = base.date and tvl_data.chain = base.chain
left join token_incentives ti on ti.date = base.date and ti.chain = base.chain
where base.date < to_date(sysdate())
