{{
    config(
        materialized='incremental',
        snowflake_warehouse=var('snowflake_warehouse', default='BANANAGUN'),
        unique_key='contract_address'
    )
}}

{{ get_bananagun_contracts('base') }}