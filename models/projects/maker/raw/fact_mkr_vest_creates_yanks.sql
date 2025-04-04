{{
    config(
        materialized="table",
        snowflake_warehouse="MAKER",
        database="maker",
        schema="raw",
        alias="fact_mkr_vest_creates_yanks"
    )
}}

-- MKR Vest Creates
SELECT
    ts,
    hash,
    32110 AS code, -- MKR expense realized
    -total_mkr AS value
FROM {{ ref('fact_mkr_vest_creates') }}

UNION ALL

SELECT
    ts,
    hash,
    33110 AS code, -- MKR in vest contracts increases
    total_mkr AS value
FROM {{ ref('fact_mkr_vest_creates') }}

UNION ALL

-- MKR Yanks
SELECT
    ts,
    hash,
    32110 AS code, -- MKR expense reversed (yanked)
    yanked_mkr AS value
FROM {{ ref('fact_mkr_yanks') }}

UNION ALL

SELECT
    ts,
    hash,
    33110 AS code, -- MKR in vest contracts yanked (decreases)
    -yanked_mkr AS value
FROM {{ ref('fact_mkr_yanks') }}