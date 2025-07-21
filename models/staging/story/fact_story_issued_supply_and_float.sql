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

gas_burns AS (
  SELECT
    b.block_date,
    SUM((b.base_fee_per_gas * tx.gas_used) / 1e18) AS burned_fee_ip
  FROM (
    SELECT
      PARSE_JSON(PARQUET_RAW):block_number::INT AS block_number,
      PARSE_JSON(PARQUET_RAW):receipt_gas_used::INT AS gas_used
    FROM {{ source("PROD_LANDING", "raw_story_transactions_parquet" ) }}
  ) tx
  JOIN (
    SELECT
      BLOCK_NUMBER,
      BASE_FEE_PER_GAS,
      BLOCK_TIMESTAMP::DATE AS block_date
    FROM {{ ref("fact_story_blocks") }}
  ) b
  ON tx.block_number = b.block_number
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

burn_dates AS (
  SELECT block_date FROM gas_burns
  UNION
  SELECT block_date FROM staking_burns
),

last_burn_date AS (
  SELECT MAX(block_date) AS max_date FROM burn_dates
),
story_price AS (
    SELECT
        date,
        shifted_token_price_usd as price
    FROM {{ ref("fact_coingecko_token_date_adjusted") }} 
    WHERE coingecko_id = 'story-2'
),

final AS (
  SELECT
    ds.date,
    COALESCE(sds.emissions, 0) AS ip_emissions,
    COALESCE(sds.initial_incentives, 0) AS initial_incentives,
    COALESCE(sds.core_contributors, 0) AS core_contributors,
    COALESCE(sds.early_backers, 0) AS early_backers,
    COALESCE(sds.total_unlocked, 0) AS total_unlocked,
    COALESCE(g.burned_fee_ip, 0) AS burned_fee_ip,
    COALESCE(s.total_burned_ip, 0) AS staking_fee_burn_ip,
    COALESCE(g.burned_fee_ip, 0) + COALESCE(s.total_burned_ip, 0) AS total_ip_burned,
    (COALESCE(g.burned_fee_ip, 0) + COALESCE(s.total_burned_ip, 0)) * p.price as revenue
  FROM date_spine ds
  LEFT JOIN {{ ref("story_daily_supply_data") }}  sds ON ds.date = sds.date
  LEFT JOIN gas_burns g ON ds.date = g.block_date
  LEFT JOIN staking_burns s ON ds.date = s.block_date
  LEFT JOIN story_price p ON ds.date = p.date
)

SELECT
  f.date,
  burned_fee_ip,
  staking_fee_burn_ip,
  total_ip_burned,
  SUM(total_ip_burned) OVER (ORDER BY f.date) AS cumulative_ip_burned,
  revenue,
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