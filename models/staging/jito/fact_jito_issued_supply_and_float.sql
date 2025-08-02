{{
    config(
        materialized="table",
        snowflake_warehouse="JITO"
    )
}}

WITH date_spine AS (
  SELECT date
  FROM {{ ref('dim_date_spine') }}
  WHERE date BETWEEN DATE '2023-12-07' AND TO_DATE(SYSDATE())
),
treasury_outflows_raw AS (
  SELECT 
    DATE(block_timestamp) AS date,
    SUM(
        CASE 
            WHEN LOWER(tx_from) = LOWER('5eosrve6LktMZgVNszYzebgmmC7BjLK8NoWyRQtcmGTF') THEN amount
            WHEN LOWER(tx_to) = LOWER('5eosrve6LktMZgVNszYzebgmmC7BjLK8NoWyRQtcmGTF') THEN -amount
            ELSE 0
        END 
    ) AS daily_outflow
  FROM {{ source('SOLANA_FLIPSIDE', 'fact_transfers') }}
  WHERE (
    LOWER(tx_from) = LOWER('5eosrve6LktMZgVNszYzebgmmC7BjLK8NoWyRQtcmGTF')
    OR LOWER(tx_to) = LOWER('5eosrve6LktMZgVNszYzebgmmC7BjLK8NoWyRQtcmGTF')
  ) 
  AND lower(mint) = lower('jtojtomepa8beP8AuQc6eXt5FriJwfFMwQx2v2f9mCL')
  GROUP BY 1
),

treasury_outflows_filled AS (
  SELECT 
    ds.date,
    COALESCE(t.daily_outflow, 0) AS daily_outflow
  FROM date_spine ds
  LEFT JOIN treasury_outflows_raw t ON ds.date = t.date
),

cumulative_treasury_outflows AS (
  SELECT 
    date,
    SUM(daily_outflow) OVER (ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_treasury
  FROM treasury_outflows_filled
),

final AS (
  SELECT 
    ds.date,
    COALESCE(u.cumulative_unlocked, 0), 
    COALESCE(t.cumulative_treasury, 0) as cumulative_treasury,
    115000000
      + COALESCE(u.cumulative_unlocked, 0)
      + COALESCE(t.cumulative_treasury, 0) AS circulating_supply
  FROM date_spine ds
  LEFT JOIN {{ ref('jito_daily_supply_data') }} u ON ds.date = u.date
  LEFT JOIN cumulative_treasury_outflows t ON ds.date = t.date
)

SELECT 
    date,
    1000000000 as max_supply,
    1000000000 as total_supply,
    0 as uncreated_tokens,
    0 as native_burns,
    532142857 + cumulative_treasury as issued_supply,
    circulating_supply
FROM final
ORDER BY date