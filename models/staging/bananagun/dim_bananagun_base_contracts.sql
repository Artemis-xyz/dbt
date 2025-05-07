{{
    config(
        materialized='incremental',
        snowflake_warehouse='BANANAGUN',
        unique_key='contract_address'
    )
}}

{{ get_bananagun_contracts('base') }}