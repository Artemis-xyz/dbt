{{
    config(
        materialized='incremental',
        snowflake_warehouse='BANANAGUN',
        unique_key='transaction_hash'
    )
}}

{{ get_bananagun_trades('ethereum') }}
