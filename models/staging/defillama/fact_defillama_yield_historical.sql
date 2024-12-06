{{ config(materialized="table") }}

WITH max_extraction AS (
        SELECT max(extraction_date) AS max_date
        FROM {{ source("PROD_LANDING", "raw_defillama_yield_historical_data") }}
    ),
    protocol_data AS (
        SELECT
            extraction_date::date as date,
            parse_json(source_json):"pool" AS pool,
            parse_json(source_json):"data" AS data
        FROM {{ source("PROD_LANDING", "raw_defillama_yield_historical_data") }}
        WHERE extraction_date = (SELECT max_date FROM max_extraction)

    )
SELECT
    date,
    pool::varchar AS pool,
    value:"apy"::float AS apy,
    value:"apyBase"::float AS apy_base,
    value:"apyBase7d"::float AS apy_base_7d,
    value:"apyReward"::float AS apy_reward,
    value:"timestamp"::date AS date
FROM protocol_data, lateral flatten(input => data)