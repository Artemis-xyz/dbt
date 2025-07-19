{{
    config(
        materialized="table"
    )
}}

WITH date_spine AS (
  SELECT 
    DATEADD(DAY, ROW_NUMBER() OVER (ORDER BY SEQ4()) - 1, DATE '2025-01-19') AS date
  FROM TABLE(GENERATOR(ROWCOUNT => 1460))
),

daily_unlocks AS (
  SELECT 
    date,
    CASE WHEN date = DATE '2025-01-19' THEN 100000000 ELSE 0 END AS initial_incentives,
    CASE 
      WHEN date = DATE '2025-01-19' THEN 100000000
      WHEN date > DATE '2025-01-19' AND DATEDIFF(DAY, DATE '2025-01-19', date) <= 1440 
        THEN (284000000.0 / 1440)
      ELSE 0 
    END AS ecosystem_community,
    CASE 
      WHEN date = DATE '2025-01-19' THEN 50000000
      WHEN date > DATE '2025-01-19' AND DATEDIFF(DAY, DATE '2025-01-19', date) <= 365 
        THEN (50000000.0 / 365)
      ELSE 0 
    END AS foundation,
    CASE 
      WHEN DATEDIFF(DAY, DATE '2025-01-19', date) > 365 
           AND DATEDIFF(DAY, DATE '2025-01-19', date) <= (365 + 1080)
        THEN (200000000.0 / 1080)
      ELSE 0 
    END AS core_contributors,
    CASE 
      WHEN DATEDIFF(DAY, DATE '2025-01-19', date) > 365 
           AND DATEDIFF(DAY, DATE '2025-01-19', date) <= (365 + 1080)
        THEN (216000000.0 / 1080)
      ELSE 0 
    END AS early_backers
  FROM date_spine
),

cumulative_unlocks AS (
  SELECT 
    date,
    SUM(initial_incentives) OVER (ORDER BY date) AS initial_incentives,
    SUM(ecosystem_community) OVER (ORDER BY date) AS ecosystem_community,
    SUM(foundation) OVER (ORDER BY date) AS foundation,
    SUM(core_contributors) OVER (ORDER BY date) AS core_contributors,
    SUM(early_backers) OVER (ORDER BY date) AS early_backers
  FROM daily_unlocks
),

unlocks_final AS (
  SELECT *,
    LEAST(
      initial_incentives + ecosystem_community + foundation + core_contributors + early_backers,
      1000000000
    ) AS total_unlocked
  FROM cumulative_unlocks
),

tx AS (
  SELECT
    PARSE_JSON(PARQUET_RAW):transaction_hash::STRING AS txn_hash,
    PARSE_JSON(PARQUET_RAW):block_number::INT AS block_number,
    PARSE_JSON(PARQUET_RAW):receipt_gas_used::INT AS gas_used
  FROM {{ source("PROD_LANDING", "raw_story_transactions_parquet" ) }}   
),

blocks AS (
  SELECT
    BLOCK_NUMBER,
    BASE_FEE_PER_GAS,
    BLOCK_TIMESTAMP::DATE AS block_date
  FROM {{ ref("fact_story_blocks") }}
),

gas_burns AS (
  SELECT
    b.block_date,
    SUM((b.base_fee_per_gas * tx.gas_used) / 1e18) AS burned_fee_ip
  FROM tx
  JOIN blocks b ON tx.block_number = b.block_number
  GROUP BY b.block_date
),

staking_burns AS (
  SELECT
    TO_DATE(parquet_raw:block_timestamp::TIMESTAMP_NTZ) AS block_date,
    SUM(parquet_raw:value::NUMBER) / 1e18 AS total_burned_ip
  FROM {{ source("PROD_LANDING", "raw_story_transactions_parquet" ) }}  
  WHERE LOWER(parquet_raw:to_address::STRING) = '0xcccccc0000000000000000000000000000000001'
    AND LEFT(parquet_raw:input::STRING, 10) IN (
      '0xd6f89acd','0x2b3d1729','0x72a02aab','0x18f4e67a',
      '0xc582db44','0x8ed65fbc','0x878f558b','0x0ef208e8',
      '0x53fc1d29','0x8ffcd279','0x28295f54'
    )
    AND parquet_raw:value::NUMBER >= 1e17
  GROUP BY block_date
),

manual_emissions AS (
  SELECT 
    DATEADD(day, seq4(), DATE '2025-03-04') AS date,
    55555 AS emissions
  FROM TABLE(GENERATOR(ROWCOUNT => 365))
),

burn_dates AS (
  SELECT block_date FROM gas_burns
  UNION
  SELECT block_date FROM staking_burns
),

last_burn_date AS (
  SELECT MAX(block_date) AS max_date FROM burn_dates
),

final AS (
  SELECT
    d.date,
    COALESCE(g.burned_fee_ip, 0) AS burned_fee_ip,
    COALESCE(s.total_burned_ip, 0) AS staking_fee_burn_ip,
    COALESCE(g.burned_fee_ip, 0) + COALESCE(s.total_burned_ip, 0) AS total_ip_burned,
    COALESCE(m.emissions, 0) AS ip_emissions,
    u.total_unlocked,
    u.initial_incentives,
    u.core_contributors,
    u.early_backers
  FROM date_spine d
  LEFT JOIN gas_burns g ON d.date = g.block_date
  LEFT JOIN staking_burns s ON d.date = s.block_date
  LEFT JOIN manual_emissions m ON d.date = m.date
  LEFT JOIN unlocks_final u ON d.date = u.date
)

SELECT
  f.date,
  burned_fee_ip,
  staking_fee_burn_ip,
  total_ip_burned,
  SUM(total_ip_burned) OVER (ORDER BY f.date) AS cumulative_ip_burned,
  ip_emissions,
  SUM(ip_emissions) OVER (ORDER BY f.date) AS cumulative_ip_emissions,
  1000000000 + 
    SUM(ip_emissions) OVER (ORDER BY f.date) -
    SUM(total_ip_burned) OVER (ORDER BY f.date) AS total_supply,
  (initial_incentives + core_contributors + early_backers) +
  SUM(ip_emissions) OVER (ORDER BY f.date) -
  SUM(total_ip_burned) OVER (ORDER BY f.date) AS circulating_supply,
  1000000000 - 484000000 + 
    SUM(ip_emissions) OVER (ORDER BY f.date) -
    SUM(total_ip_burned) OVER (ORDER BY f.date) AS issued_supply
FROM final f
JOIN last_burn_date lbd ON f.date <= lbd.max_date
ORDER BY f.date