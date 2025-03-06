-- depends_on: {{ ref('fact_avalanche_transactions_v2') }}
{{
    config(
        materialized="incremental",
        unique_key="tx_hash",
        snowflake_warehouse="AVALANCHE_MD",
        database="avalanche",
        schema="raw",
        alias="ez_transactions_v2",
    )
}}

{{ create_ez_transactions("avalanche", "v2") }}
