{{
    config(
        materialized='table',
        snowflake_warehouse='BANANAGUN',
        unique_key=['transaction_hash','index']
    )
}}

{{ get_bananagun_fees('blast') }}
