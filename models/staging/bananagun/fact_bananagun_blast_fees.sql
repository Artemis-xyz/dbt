{{
    config(
        materialized='incremental',
        snowflake_warehouse=var('snowflake_warehouse', default='BANANAGUN'),
        unique_key=['transaction_hash','index']
    )
}}

{{ get_bananagun_fees('blast') }}
