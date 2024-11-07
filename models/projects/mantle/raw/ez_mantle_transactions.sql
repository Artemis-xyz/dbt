-- depends_on: {{ ref('fact_mantle_transactions') }}
{{
    config(
        materialized="incremental",
        unique_key="tx_hash",
        snowflake_warehouse="mantle",
        database="mantle",
        schema="raw",
        alias="ez_transactions",
    )
}}

{{ create_ez_transactions("mantle") }}