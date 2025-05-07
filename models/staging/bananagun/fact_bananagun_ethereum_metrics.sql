{{
    config(
        materialized='incremental',
        snowflake_warehouse='BANANAGUN',
        unique_key='trade_date'
    )
}}

{{ get_bananagun_metrics('ethereum') }}
