-- depends_on: {{ ref('fact_avalanche_transactions') }}
{{
    config(
        materialized="incremental",
        unique_key="tx_hash",
        snowflake_warehouse="AVALANCHE",
        database="avalanche",
        schema="raw",
        alias="ez_transactions",
    )
}}

{{ create_ez_transactions("avalanche") }}
