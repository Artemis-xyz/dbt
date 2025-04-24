{{ config(materialized="table") }}

WITH parsed_data AS (
  SELECT
    DATE(EXTRACTION_DATE) as date,
    EXTRACTION_DATE,
    PARSE_JSON(SOURCE_JSON) as json_data
  from {{ source("PROD_LANDING", "raw_hivemapper_KMs") }}
),
ranked_data AS (
  SELECT
    date,
    EXTRACTION_DATE,
    json_data,
    ROW_NUMBER() OVER (PARTITION BY date ORDER BY EXTRACTION_DATE DESC) as rn
  FROM parsed_data
), current_json AS (
  SELECT * 
  FROM ranked_data
  WHERE rn = 1
),
flattened_data AS (
  SELECT
    key as date,
    value:totalContributors::FLOAT as total_contributors,
    value:totalEvents::FLOAT as total_events,
    value:totalKm::FLOAT as total_km,
    value:totalUniqueKm::FLOAT as total_unique_km,
    value:totalRewardTransactions::FLOAT as total_reward_transactions,
    value:totalAiTrainerReviews::FLOAT as total_ai_trainer_reviews
  FROM current_json,
  LATERAL FLATTEN(input => json_data:stats:byDay) f
)
SELECT
  TO_DATE(date) as date,
  total_km,
  total_unique_km
FROM flattened_data
ORDER BY date