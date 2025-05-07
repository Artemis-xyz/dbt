{{
    config(
        materialized='incremental',
        snowflake_warehouse='BANANAGUN',
        unique_key=['transaction_hash','index']
    )
}}

{{ get_bananagun_fees('solana') }}
