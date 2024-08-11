{{
    config(
        materialized="table",
        snowflake_warehouse="UNISWAP_SM",
        database="uniswap",
        schema="core",
        alias="ez_metrics_by_chain",
    )
}}

with
    trading_volume_by_pool as (
        {{
            dbt_utils.union_relations(
                relations=[
                    ref("fact_uniswap_v2_ethereum_trading_vol_fees_traders_by_pool"),
                    ref("fact_uniswap_v3_arbitrum_trading_vol_fees_traders_by_pool"),
                    ref("fact_uniswap_v3_avalanche_trading_vol_fees_traders_by_pool"),
                    ref("fact_uniswap_v3_base_trading_vol_fees_traders_by_pool"),
                    ref("fact_uniswap_v3_blast_trading_vol_fees_traders_by_pool"),
                    ref("fact_uniswap_v3_bsc_trading_vol_fees_traders_by_pool"),
                    ref("fact_uniswap_v3_ethereum_trading_vol_fees_traders_by_pool"),
                    ref("fact_uniswap_v3_optimism_trading_vol_fees_traders_by_pool"),
                    ref("fact_uniswap_v3_polygon_trading_vol_fees_traders_by_pool"),
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
    , token_incentives_cte as (
        SELECT
            date
            , 'ethereum' as chain
            , token_incentives_usd
        FROM {{ ref('fact_uniswap_token_incentives') }}
    )
    , treasury_cte AS(
        SELECT
            date
            , 'ethereum' as chain
            , sum(usd_balance) as treasury
        FROM {{ ref('fact_uniswap_treasury_by_token') }}
        GROUP BY 1, 2
    )
    , treasury_native_cte AS(
        SELECT
            date
            , 'ethereum' as chain
            , treasury_native
        FROM {{ ref('fact_uniswap_treasury_by_token') }}
        WHERE token = 'UNI'
    )
    , net_treasury_cte AS(
        SELECT
            date
            , 'ethereum' as chain
            , sum(usd_balance) as net_treasury
        FROM {{ ref('fact_uniswap_treasury_by_token') }}
        WHERE token <> 'UNI'
        GROUP BY 1, 2
    )
    , tvl_by_pool as (
        {{
            dbt_utils.union_relations(
                relations=[
                    ref("fact_uniswap_v2_ethereum_tvl_by_pool"),
                    ref("fact_uniswap_v3_arbitrum_tvl_by_pool"),
                    ref("fact_uniswap_v3_avalanche_tvl_by_pool"),
                    ref("fact_uniswap_v3_base_tvl_by_pool"),
                    ref("fact_uniswap_v3_blast_tvl_by_pool"),
                    ref("fact_uniswap_v3_bsc_tvl_by_pool"),
                    ref("fact_uniswap_v3_ethereum_tvl_by_pool"),
                    ref("fact_uniswap_v3_optimism_tvl_by_pool"),
                    ref("fact_uniswap_v3_polygon_tvl_by_pool"),
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
select
    tvl_by_chain.date
    , 'uniswap' as app
    , 'DeFi' as category
    , tvl_by_chain.chain
    , trading_fees
    , trading_fees as fees
    , trading_fees as primary_supply_side_revenue
    , trading_fees as total_supply_side_revenue
    , 0 as other_supply_side_revenue
    , 0 as protocol_revenue
    , 0 as operating_expenses
    , token_incentives_usd
    , -token_incentives_usd as protocol_earnings
    , tvl_by_chain.tvl
    , treasury
    , treasury_native
    , net_treasury
    , tvl_by_chain.tvl as net_deposit
    , trading_volume_by_chain.trading_volume
    , trading_volume_by_chain.unique_traders
    , trading_volume_by_chain.gas_cost_native
    , trading_volume_by_chain.gas_cost_usd
from tvl_by_chain
left join token_incentives_cte using(date, chain)
left join trading_volume_by_chain using(date, chain)
left join treasury_cte using(date, chain)
left join treasury_native_cte using(date, chain)
left join net_treasury_cte using(date, chain)
where tvl_by_chain.date < to_date(sysdate())
order by 1 desc