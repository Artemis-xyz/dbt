{{
    config(
        materialized='table',
        snowflake_warehouse='SYNTHETIX',
        database='synthetix',
        schema='raw',
        alias='fact_synthetix_ethereum_token_holders'    
    )
}}

{{ token_holders('ethereum', '0xC011a73ee8576Fb46F5E1c5751cA3B9Fe0af2a6F', '2024-02-23') }}
