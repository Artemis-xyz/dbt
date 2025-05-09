{{
    config(
        materialized="table",
    )
}}

WITH parsed_data AS (
  SELECT
    EXTRACTION_DATE,
    PARSE_JSON(SOURCE_JSON) as json_data
  FROM {{ source('PROD_LANDING', 'raw_meteora_lbpairs') }}
),
latest_extraction AS (
  SELECT MAX(EXTRACTION_DATE) as latest_date
  FROM parsed_data
),
latest_data AS (
  SELECT
    EXTRACTION_DATE,
    json_data
  FROM parsed_data
  WHERE EXTRACTION_DATE = (SELECT latest_date FROM latest_extraction)
),
flattened_data AS (
  SELECT
    d.EXTRACTION_DATE,
    p.value as pair_data
  FROM latest_data d,
  LATERAL FLATTEN(input => d.json_data) p
)
SELECT
  distinct pair_data:address::STRING as address,
  CONCAT('Meteora (', pair_data:name::STRING, ') LBPair Market') as name,
  'meteora' as artemis_application_id,
  'solana' as chain,
  null as is_token,
  null as is_fungible,
  'spot_pool' as type,
  SYSDATE()::TIMESTAMP_NTZ as last_updated
FROM flattened_data
