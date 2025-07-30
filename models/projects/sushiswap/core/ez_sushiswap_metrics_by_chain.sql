{{
    config(
        materialized="table",
        snowflake_warehouse="SUSHISWAP_SM",
        database="sushiswap",
        schema="core",
        alias="ez_metrics_by_chain",
    )
}}

with trading_volume_by_pool as (
    {{
        dbt_utils.union_relations(
            relations=[
                ref("fact_sushiswap_v2_arbitrum_trading_vol_fees_traders_by_pool"),
                ref("fact_sushiswap_v2_avalanche_trading_vol_fees_traders_by_pool"),
                ref("fact_sushiswap_v2_bsc_trading_vol_fees_traders_by_pool"),
                ref("fact_sushiswap_v2_ethereum_trading_vol_fees_traders_by_pool"),
                ref("fact_sushiswap_v2_gnosis_trading_vol_fees_traders_by_pool"),
            ],
        )
    }}
)
, trading_volume_by_chain as (
    select
        trading_volume_by_pool.date,
        trading_volume_by_pool.chain,
        sum(trading_volume_by_pool.trading_volume) as trading_volume,
        sum(trading_volume_by_pool.trading_fees) as trading_fees,
        sum(trading_volume_by_pool.unique_traders) as unique_traders,
        sum(trading_volume_by_pool.gas_cost_native) as gas_cost_native,
        sum(trading_volume_by_pool.gas_cost_usd) as gas_cost_usd
    from trading_volume_by_pool
    group by trading_volume_by_pool.date, trading_volume_by_pool.chain
)
, tvl_by_pool as (
    {{
        dbt_utils.union_relations(
            relations=[
                ref("fact_sushiswap_v2_gnosis_tvl_by_pool"),
                ref("fact_sushiswap_v2_ethereum_tvl_by_pool"),
                ref("fact_sushiswap_v2_bsc_tvl_by_pool"),
                ref("fact_sushiswap_v2_avalanche_tvl_by_pool"),
                ref("fact_sushiswap_v2_arbitrum_tvl_by_pool"),

            ],
        )
    }}
)
, tvl_by_chain as (
    select
        tvl_by_pool.date,
        tvl_by_pool.chain,
        sum(tvl_by_pool.tvl) as tvl
    from tvl_by_pool
    group by tvl_by_pool.date, tvl_by_pool.chain
)
, cashflow_metrics as (
    select
        date,
        chain,
        sum(trading_fees) as fees,
        case
            when date between '2023-01-23' and '2024-01-23' THEN
                sum(trading_fees * 0.0030)
            else
                sum(trading_fees * 0.0025 / 0.0030)
        end as service_fee_allocation,
        case
            when date between '2023-01-23' and '2024-01-23' THEN
                sum(0)
            else
                sum(trading_fees * 0.0005 / 0.0030)
        end as staking_fee_allocation
    from trading_volume_by_chain
    group by date, chain
)
, sushiswap_metric_dates as (
    select
        date,
        chain
    from {{ ref('fact_sushiswap_token_incentives') }}
    group by date, chain
)
, token_incentives as (
    select
        date,
        chain,
        sum(incentives_usd) as token_incentives
    from {{ ref('fact_sushiswap_token_incentives') }}
    group by date, chain
)

select
    sushiswap_metric_dates.date
    , 'sushiswap' as artemis_id
    , sushiswap_metric_dates.chain

    --Usage Data
    , trading_volume_by_chain.unique_traders as spot_dau
    , trading_volume_by_chain.unique_traders as dau
    , tvl_by_chain.tvl
    , trading_volume_by_chain.trading_volume as spot_volume
    , trading_volume_by_chain.gas_cost_native
    , trading_volume_by_chain.gas_cost_usd

    --Fee Data
    , trading_volume_by_chain.trading_fees as spot_fees
    , cashflow_metrics.fees as fees

    --Fee Allocation
    , cashflow_metrics.service_fee_allocation as lp_fee_allocation
    , cashflow_metrics.staking_fee_allocation as staking_fee_allocation
    
    --Financial Statement
    , 0 as revenue_native
    , 0 as revenue
    , token_incentives.token_incentives as token_incentives
    , revenue - token_incentives as earnings

from sushiswap_metric_dates
left join tvl_by_chain using(date, chain)
left join trading_volume_by_chain using(date, chain)
left join cashflow_metrics using(date, chain)
left join token_incentives using(date, chain)
where sushiswap_metric_dates.date < to_date(sysdate())
