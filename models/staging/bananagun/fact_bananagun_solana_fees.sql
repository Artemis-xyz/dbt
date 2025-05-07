{{
    config(
        materialized='incremental',
        snowflake_warehouse='ANALYTICS_XL',
        unique_key=['transaction_hash','index']
    )
}}

{{ get_bananagun_fees('solana') }}
