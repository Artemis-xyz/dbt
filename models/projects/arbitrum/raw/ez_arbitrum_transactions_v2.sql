-- depends_on: {{ ref('fact_arbitrum_transactions_v2') }}
{{
    config(
        materialized="incremental",
        unique_key="tx_hash",
        snowflake_warehouse="ARBITRUM_LG",
        database="arbitrum",
        schema="raw",
        alias="ez_transactions_v2",
    )
}}

{{ create_ez_transactions("arbitrum", "v2") }}
