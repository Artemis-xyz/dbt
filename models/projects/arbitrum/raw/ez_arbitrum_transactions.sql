-- depends_on: {{ ref('fact_arbitrum_transactions') }}
{{
    config(
        materialized="incremental",
        unique_key="tx_hash",
        snowflake_warehouse="ARBITRUM_MD",
        database="arbitrum",
        schema="raw",
        alias="ez_transactions",
    )
}}

{{ create_ez_transactions("arbitrum") }}
