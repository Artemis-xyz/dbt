-- depends_on: {{ ref('fact_base_transactions') }}
{{
    config(
        materialized="incremental",
        unique_key="tx_hash",
        snowflake_warehouse="base",
        database="base",
        schema="raw",
        alias="ez_transactions",
    )
}}

{{ create_ez_transactions("base") }}
