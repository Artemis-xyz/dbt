{{
    config(
        materialized='table',
        snowflake_warehouse='USUAL'
    )
}}

WITH user_balance AS (
  SELECT 
    date,
    blockchain,
    address,
    contract_address,
    SUM(SUM(amount)) OVER (PARTITION BY address, blockchain, contract_address ORDER BY date) AS daily_cumulative_balance
  FROM (
    -- Ethereum Transfers for USD0 (Outflows)
    SELECT 
      DATE(block_timestamp) AS date,
      'ethereum' AS blockchain,
      LOWER(decoded_log:"from") AS address,
      LOWER(contract_address) AS contract_address,
      -CAST(decoded_log:"value" AS NUMERIC) / 1e18 AS amount
    FROM 
      ethereum_flipside.core.ez_decoded_event_logs
    WHERE 
      LOWER(decoded_log:"from") != '0x0000000000000000000000000000000000000000'
      AND LOWER(contract_address) IN ('0x73a15fed60bf67631dc6cd7bc5b6e8da8190acf5', '0x35d8949372d46b7a3d5a56006ae77b215fc69bc0')
      
    UNION ALL
    
    -- Ethereum Transfers for USD0++ (Inflows)
    SELECT 
      DATE(block_timestamp) AS date,
      'ethereum' AS blockchain,
      LOWER(decoded_log:"to") AS address,
      LOWER(contract_address) AS contract_address,
      CAST(decoded_log:"value" AS NUMERIC) / 1e18 AS amount
    FROM 
      ethereum_flipside.core.ez_decoded_event_logs
    WHERE 
      LOWER(decoded_log:"to") != '0x0000000000000000000000000000000000000000'
      AND LOWER(contract_address) IN ('0x73a15fed60bf67631dc6cd7bc5b6e8da8190acf5', '0x35d8949372d46b7a3d5a56006ae77b215fc69bc0')
  ) x
  GROUP BY 1, 2, 3, 4
),

setLeadData AS (
  SELECT 
    *, 
    LEAD(date, 1, CURRENT_DATE) OVER (PARTITION BY address, blockchain, contract_address ORDER BY date) AS latest_day 
  FROM 
    user_balance
),

gs AS (
  SELECT 
    DATEADD(day, ROW_NUMBER() OVER (ORDER BY NULL) - 1, DATE_TRUNC('day', DATEADD(year, -1, CURRENT_DATE))) AS date
  FROM 
    TABLE(GENERATOR(ROWCOUNT => 365))
),

getUserDailyBalance AS (
  SELECT 
    gs.date,
    g.blockchain,
    g.address,
    g.contract_address,
    g.daily_cumulative_balance
  FROM 
    setLeadData g
  JOIN 
    gs ON g.date <= gs.date AND gs.date < g.latest_day
  WHERE 
    g.daily_cumulative_balance > 1 / 1e12
),

inter AS (
  SELECT 
    date, 
    CASE 
      WHEN contract_address = '0x73a15fed60bf67631dc6cd7bc5b6e8da8190acf5' THEN 'USD0'
      WHEN contract_address = '0x35d8949372d46b7a3d5a56006ae77b215fc69bc0' THEN 'USD0++'
    END AS token,
    daily_cumulative_balance
  FROM getUserDailyBalance
)

SELECT 
  date,
  SUM(CASE WHEN token = 'USD0' THEN daily_cumulative_balance ELSE 0 END) AS usd0_tvl,
  SUM(CASE WHEN token = 'USD0++' THEN daily_cumulative_balance ELSE 0 END) AS usd0pp_tvl
FROM 
  inter
GROUP BY 
  date
ORDER BY 
  date DESC