-- depends_on: {{ ref('fact_celo_decoded_events') }}
{{
    config(
        materialized="table",
        snowflake_warehouse="CELO",
        database="celo",
        schema="raw",
        alias="ez_decoded_events",
    )
}}
{{ get_ez_decoded_events("celo") }}