-- depends_on: {{ ref('fact_optimism_transactions') }}
{{
    config(
        materialized="incremental",
        unique_key="tx_hash",
        snowflake_warehouse="optimism",
        database="optimism",
        schema="raw",
        alias="ez_transactions",
    )
}}

{{ create_ez_transactions("optimism") }}
