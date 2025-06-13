{{
    config(
        materialized='incremental',
        snowflake_warehouse=var('snowflake_warehouse', default='BANANAGUN'),
        unique_key='trade_date'
    )
}}

{{ get_bananagun_metrics('solana') }}
