{{
    config(
        materialized='table',
        snowflake_warehouse='SYNTHETIX',
        database='synthetix',
        schema='raw',
        alias='fact_synthetix_optimism_token_holders'
    )
}}

{{ token_holders('optimism', '0x8700dAec35aF8Ff88c16BdF0418774CB3D7599B4', '2024-07-21') }}
