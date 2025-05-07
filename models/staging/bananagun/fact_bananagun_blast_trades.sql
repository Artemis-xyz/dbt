{{
    config(
        materialized='table',
        snowflake_warehouse='BANANAGUN',
        unique_key='transaction_hash'
    )
}}

{{ get_bananagun_trades('blast') }}
