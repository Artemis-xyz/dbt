{{
    config(
        materialized='incremental',
        snowflake_warehouse=var('snowflake_warehouse', default='BANANAGUN'),
        unique_key='transaction_hash'
    )
}}

{{ get_bananagun_trades('solana') }}