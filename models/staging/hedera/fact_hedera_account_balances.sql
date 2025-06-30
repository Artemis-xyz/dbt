
{{ config(materialized="table") }}

WITH raw_parsed AS (
  SELECT
    TO_TIMESTAMP_TZ(extraction_date) AS extraction_date,
    PARSE_JSON(source_json) AS json_data
  FROM {{ source("PROD_LANDING", "raw_hbar_allocations_by_account") }}
)
, with_snapshot_key AS (
  SELECT
    extraction_date,
    OBJECT_KEYS(json_data)[0]::DATE AS snapshot_date,
    json_data
  FROM raw_parsed
),

flattened AS (
  SELECT
    extraction_date,
    snapshot_date::DATE AS snapshot_date,
    f.value:"account_id"::STRING AS account,
    f.value:"hbar_balance"::FLOAT AS balance
  FROM with_snapshot_key,
       LATERAL FLATTEN(input => json_data[snapshot_date]) f
)

, ranked AS (
  SELECT *,
         ROW_NUMBER() OVER (PARTITION BY snapshot_date, account ORDER BY extraction_date DESC) AS rn
  FROM flattened
)

SELECT
  snapshot_date,
  account,
  balance
FROM ranked
WHERE rn = 1