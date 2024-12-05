WITH max_extraction AS (
        SELECT max(extraction_date) AS max_date
        FROM {{ source("PROD_LANDING", "raw_defillama_yield_data") }}
    ),
    protocol_data AS (
        SELECT
            extraction_date::date as date,
            parse_json(source_json):"data" AS data
        FROM {{ source("PROD_LANDING", "raw_defillama_yield_data") }}
        WHERE extraction_date = (SELECT max_date FROM max_extraction)

    )
SELECT
    date,
    value:"chain"::string AS chain,
    value:"project"::string AS project,
    value:"symbol"::string AS symbol,
    value:"tvlUsd"::float AS tvl_usd,
    value:"apyBase"::float AS apy_base,
    value:"apyReward"::float AS apy_reward,
    value:"apy"::float AS apy,
    value:"pool"::string AS pool,
    value:"apyPct1D"::string AS apy_pct_1d,
    value:"apyPct7D"::string AS apy_pct_7d,
    value:"apyPct30D"::string AS apy_pct_30d,
    value:"stablecoin"::boolean AS is_stablecoin,
    value:"apyBase7d"::string AS apy_base_7d,
    value:"apyMean30d"::string AS apy_mean_30d
FROM protocol_data, lateral flatten(input => data)
