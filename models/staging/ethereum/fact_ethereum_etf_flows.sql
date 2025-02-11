{{
    config(
        materialized='table',
        snowflake_warehouse='ETHEREUM'
    )
}}

-- Credit to @hildobby for the original version of this model: https://dune.com/queries/4208557/7083511

WITH time_series AS (
    {{
        dbt_utils.date_spine(
            datepart="day",
            start_date="cast('2017-12-11' as date)",
            end_date="to_date(sysdate())"
        )
    }}
)
, full_date_spine AS (
    {{
        dbt_utils.date_spine(
            datepart="day",
            start_date="cast('2000-01-01' as date)",
            end_date="to_date(sysdate())"
        )
    }}
) , flows AS (
    SELECT 
        date_trunc('day', t.block_timestamp) AS date
        , a.issuer
        , SUM(CASE WHEN a.inverse_values = true THEN -t.amount ELSE t.amount END) AS amount
    FROM {{ source('ETHEREUM_FLIPSIDE', 'ez_native_transfers') }} t
    INNER JOIN {{ ref('fact_ethereum_etf_addresses') }} a ON a.address=t.to_address
        AND a.track_inflow = true
    WHERE t.block_timestamp >= date('2017-10-16')
    GROUP BY 1, 2
    
    UNION ALL
    
    SELECT 
        date_trunc('day', t.block_timestamp) AS date
        , a.issuer
        , -1 * SUM(CASE WHEN a.inverse_values = true THEN -t.amount ELSE t.amount END) AS amount
    FROM {{ source('ETHEREUM_FLIPSIDE', 'ez_native_transfers') }} t
    INNER JOIN {{ ref('fact_ethereum_etf_addresses') }} a ON a.address=t.from_address
        AND a.track_outflow = true
    WHERE t.block_timestamp >= date('2017-10-16')
    AND t.block_timestamp < (select ethereum_update_threshold from {{ ref('fact_ethereum_etf_update_threshold') }})
    GROUP BY 1, 2
        
    UNION ALL
    
    SELECT 
        date AS time
        , 'Fidelity' AS issuer
        , -amount AS amount
    FROM {{ ref('fact_ethereum_etf_fidelity_outflows') }}    
)

, expanded_flows AS (
    SELECT 
        ts.date
        , i.issuer
        , COALESCE(SUM(f.amount), 0) AS amount
    FROM time_series ts
    CROSS JOIN (SELECT issuer FROM {{ ref('fact_ethereum_etf_metadata') }}) i
    LEFT JOIN flows f ON ts.date = f.date AND i.issuer = f.issuer
    GROUP BY 1, 2
)
, cumulative as (
    SELECT
        *,
        SUM(amount) OVER (PARTITION BY issuer ORDER BY date asc) as cum_amount
    FROM expanded_flows
)
, eth_prices as (
    SELECT
        d.date,
        p.price
    FROM
        {{ source('ETHEREUM_FLIPSIDE_PRICE', 'ez_prices_hourly') }} p
    JOIN full_date_spine d on p.hour = d.date and p.is_native
)
SELECT
    c.date,
    c.issuer,
    amount as net_etf_flow_native,
    cum_amount as cumulative_etf_flow_native,
    amount * p.price as net_etf_flow,
    cum_amount * p.price as cumulative_etf_flow
FROM cumulative c
LEFT JOIN eth_prices p on p.date = c.date