{{
    config(
        materialized='table',
        snowflake_warehouse=var('snowflake_warehouse', default='BANANAGUN')
    )
}}

{{ get_bananagun_trades('base') }}
