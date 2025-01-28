{{
    config(
        materialized='table',
        snowflake_warehouse='BALANCER',
        database='BALANCER',
        schema='core',
        alias='ez_metrics_by_chain'
    )
}}
/*
    with all_tvl_by_chain as (
        SELECT
            date,
            chain,
            sum(tvl_native) as tvl_native,
            sum(tvl_usd) as tvl_usd
        FROM {{ ref('fact_balancer_tvl_by_chain_and_token') }}
        group by 1,2
),
*/
with tvl_balancer_v1 as (
    SELECT
        date,
        'ethereum' as chain,
        SUM(tvl_token_adjusted) as tvl_usd
    FROM {{ ref('fact_balancer_liquidity') }}
    group by 1,2
)

, treasury_by_chain as (
    SELECT
        date,
        'ethereum' as chain,
        sum(usd_balance) as usd_balance
    FROM {{ ref('fact_balancer_treasury_by_token') }}
    group by 1,2
)
, net_treasury as(
    SELECT
        date,
        'ethereum' as chain,
        sum(usd_balance) as net_treasury_usd
    FROM {{ ref('fact_balancer_treasury_by_token') }}
    where token <> 'BAL'
    group by 1,2
)
, treasury_native as (
    SELECT
        date,
        chain,
        sum(native_balance) as treasury_native
    FROM {{ ref('fact_balancer_treasury_by_token') }}
    where token = 'BAL'
    group by 1,2
)
, date_chain_spine as (
    SELECT
        distinct
        date,
        chain
    FROM {{ ref('dim_date_spine') }}
    CROSS JOIN (SELECT distinct chain from treasury_by_chain
        UNION
        --SELECT distinct chain from all_tvl_by_chain
        --UNION
        SELECT distinct chain from treasury_native
        UNION
        SELECT distinct chain from net_treasury
    )
    where date between '2020-03-01' and to_date(sysdate())
)
,   trading_metrics_by_chain AS (
        SELECT 
            block_date AS date,
            blockchain AS chain,
            version,
            COUNT(*) AS swap_count,
            SUM(swap_fee_usd) AS trading_fees,
            SUM(swap_fee_usd) AS fees, --total fees == trading fees 
            SUM(swap_fee_usd) AS primary_supply_side_revenue,
            0 AS secondary_supply_side_revenue,
            SUM(swap_fee_usd) AS total_supply_side_revenue,
            0 AS protocol_revenue,
            0 AS operating_expenses,
            0 AS token_incentives,        -- to verify
            0 AS protocol_earnings,       -- to verify
            SUM(token_sold_amount_usd) AS trading_volume,
            COUNT(DISTINCT taker) AS unique_traders,
            'TBD' AS gas_cost_native,
            'TBD' AS gas_cost_usd
        FROM {{ ref('fact_balancer_trades') }}
        WHERE NOT (token_sold_amount_raw > 9E25 AND token_sold_amount_usd > 10000000000)
        GROUP BY block_date, blockchain, version
)

select
    date_chain_spine.date,
    date_chain_spine.chain,
    trading_metrics_by_chain.version,
    trading_metrics_by_chain.swap_count,
    trading_metrics_by_chain.trading_fees,
    trading_metrics_by_chain.fees,
    trading_metrics_by_chain.primary_supply_side_revenue,
    trading_metrics_by_chain.secondary_supply_side_revenue,
    trading_metrics_by_chain.total_supply_side_revenue,
    trading_metrics_by_chain.protocol_revenue,
    trading_metrics_by_chain.operating_expenses,
    trading_metrics_by_chain.token_incentives,
    trading_metrics_by_chain.protocol_earnings,
    --all_tvl_by_chain.tvl_usd as tvl,
    tvl_balancer_v1.tvl_usd,
    treasury_by_chain.usd_balance as treasury_value,
    treasury_native.treasury_native as treasury_native,
    net_treasury.net_treasury_usd as net_treasury_value,
    trading_metrics_by_chain.trading_volume,
    trading_metrics_by_chain.unique_traders
from date_chain_spine
--left join all_tvl_by_chain using (date, chain)
left join treasury_by_chain using (date, chain)
left join treasury_native using (date, chain)
left join net_treasury using (date, chain)
left join trading_metrics_by_chain using (date, chain)
left join tvl_balancer_v1 using (date, chain)