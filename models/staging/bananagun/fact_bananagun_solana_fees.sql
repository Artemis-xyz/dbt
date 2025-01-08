{{
    config(
        materialized='incremental',
        snowflake_warehouse='BANANAGUN'
    )
}}

{{ get_bananagun_fees('solana') }}
