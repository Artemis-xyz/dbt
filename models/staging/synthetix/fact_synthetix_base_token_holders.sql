{{
    config(
        materialized='table',
        snowflake_warehouse='SYNTHETIX',
        database='synthetix',
        schema='raw',
        alias='fact_synthetix_base_token_holders'
    )
}}

{{ token_holders('base', '0x22e6966B799c4D5B13BE962E1D117b56327FDa66', '2023-12-19') }}
