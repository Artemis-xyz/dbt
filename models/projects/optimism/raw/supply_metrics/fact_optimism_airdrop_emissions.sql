{{
    config(
        materialized="table",
        snowflake_warehouse="OPTIMISM",
        database="optimism",
        schema="raw",
        alias="fact_optimism_airdrop_emissions",
    )
}}

SELECT
    '2022-05-31' AS date,
    'Airdrop' AS event_type,
    214748364 AS amount,
    'https://community.optimism.io/op-token/airdrops/airdrop-1' AS source
UNION ALL
SELECT
    '2023-02-09' AS date,
    'Airdrop' AS event_type,
    11742277 AS amount,
    'https://community.optimism.io/op-token/airdrops/airdrop-2' AS source
UNION ALL
SELECT
    '2023-09-18' AS date,
    'Airdrop' AS event_type,
    19411313 AS amount,
    'https://community.optimism.io/op-token/airdrops/airdrop-3' AS source
UNION ALL
SELECT
    '2024-02-20' AS date,
    'Airdrop' AS event_type,
    10343758 AS amount,
    'https://community.optimism.io/op-token/airdrops/airdrop-4' AS source
UNION ALL
SELECT
    '2024-10-09' AS date,
    'Airdrop' AS event_type,
    10400000 AS amount,
    'https://community.optimism.io/op-token/airdrops/airdrop-5' AS source