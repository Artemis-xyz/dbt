{{
    config(
        materialized='table',
        snowflake_warehouse='BITCOIN'
    )
}}

-- Credit to @hildobby for the original version of this model: https://dune.com/queries/3430945

WITH unaggregated AS (
    SELECT 
        i.block_timestamp
        , a.issuer
        , eat.ticker AS etf_ticker
        , 'deposit' AS flow_type
        , CASE WHEN inverse_values::boolean THEN -i.value ELSE i.value END AS amount
    FROM {{ source('BITCOIN_FLIPSIDE', 'fact_outputs') }} i
    INNER JOIN {{ ref('fact_bitcoin_etf_addresses') }} -- Known ETF address list
        a ON a.address::string = i.PUBKEY_SCRIPT_ADDRESS::string
        AND a.track_inflow
    INNER JOIN {{ ref('fact_bitcoin_etf_metadata') }} eat ON a.issuer=eat.issuer
    WHERE i.value > 0
    and i.block_timestamp > '2019-07-24'::date
    
    UNION ALL
    
    SELECT
        i.block_timestamp
        , a.issuer
        , eat.ticker AS etf_ticker
        , 'withdrawal' AS flow_type
        , CASE WHEN inverse_values::boolean THEN i.value ELSE -i.value END AS amount
    FROM {{ source('BITCOIN_FLIPSIDE', 'fact_inputs') }} i
    INNER JOIN {{ ref('fact_bitcoin_etf_addresses') }} -- Known ETF address list
        a ON a.address= i.PUBKEY_SCRIPT_ADDRESS::string
        AND a.track_outflow
    INNER JOIN {{ ref('fact_bitcoin_etf_metadata') }} eat ON a.issuer=eat.issuer
    WHERE i.value > 0
    AND i.block_timestamp > '2019-07-24'::date
    
    UNION ALL
    
    SELECT
        date::date AS block_time
        , 'Fidelity' AS issuer
        , 'FBTC' AS etf_ticker
        , 'withdrawal' AS flow_type
        , -amount AS amount
    FROM {{ ref('fact_bitcoin_etf_fidelity_outflows') }}
    
    UNION ALL
    
    SELECT 
        t.date as block_timestamp
        , et.issuer
        , et.ticker AS etf_ticker
        , NULL AS flow_type
        , NULL AS amount
    FROM {{ ref('dim_date_spine') }} t
    CROSS JOIN {{ ref('fact_bitcoin_etf_metadata') }} et
    WHERE t.date between '2019-07-24' and to_date(sysdate())
    )

, daily_aggregates AS (
    SELECT 
        f.block_timestamp::date AS date
        , f.issuer
        , f.etf_ticker
        , SUM(f.amount) AS amount
    FROM unaggregated f
    GROUP BY 1, 2, 3
)
, summed as (
     SELECT 
        date
        , issuer
        , etf_ticker
        , SUM(amount) AS amount
    FROM daily_aggregates
    GROUP BY 1, 2, 3
)
, cumulative_native as (
    SELECT
        *,
        SUM(amount) OVER (PARTITION BY issuer, etf_ticker ORDER BY date asc) as cum_amount,
    FROM summed
)
, bitcoin_prices as (
    SELECT
        date(hour) as date,
        avg(price) as price
    FROM bitcoin_flipside.price.ez_prices_hourly
    WHERE is_native = True
    GROUP BY 1
)

SELECT
    c.date,
    c.issuer,
    c.etf_ticker,
    c.amount as net_etf_flow_native,
    c.cum_amount as cumulative_etf_flow_native,
    c.amount * p.price as net_etf_flow,
    c.cum_amount * p.price as cumulative_etf_flow
FROM cumulative_native c
LEFT JOIN bitcoin_prices p ON p.date = c.date