{{
    config(
        materialized='table',
        snowflake_warehouse='BANANAGUN'
    )
}}

{{ get_bananagun_metrics('solana') }}
