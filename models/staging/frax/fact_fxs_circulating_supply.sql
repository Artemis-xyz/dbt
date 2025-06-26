{{ config(materialized="table") }}

-- Assuming the JSON data is stored in a table called API_RESPONSES with a column called RESPONSE_DATA
-- and a column called EXTRACTION_DATE

WITH latest_extraction AS (
  -- Get the latest extraction date
  SELECT 
    MAX(EXTRACTION_DATE) AS latest_date
  FROM {{source("PROD_LANDING", "raw_frax_circulating_supply")}}
),

fxs_data AS (
  -- Parse the FXS data from the JSON response
  SELECT 
    EXTRACTION_DATE,
    r.value AS item
  FROM {{source("PROD_LANDING", "raw_frax_circulating_supply")}} a,
  LATERAL FLATTEN(input => PARSE_JSON(a.SOURCE_JSON):fxs:full_items) r
  WHERE a.EXTRACTION_DATE = (SELECT latest_date FROM latest_extraction)
)

-- Extract the data and convert timestamps
SELECT 
  date_trunc('day',TO_TIMESTAMP_NTZ(item:timestamp::number)) AS date,
  item:coin::string AS coin,
  item:blockNum::number AS block_num,
  item:price::float AS price,
  item:supply::float AS supply,
  item:circulating_supply::float AS circulating_supply,
  item:market_cap::float AS market_cap,
  item:circulating_market_cap::float AS circulating_market_cap,
  item:fxs_burned_cumulative_sum::float AS fxs_burned_cumulative_sum
FROM fxs_data
ORDER BY date