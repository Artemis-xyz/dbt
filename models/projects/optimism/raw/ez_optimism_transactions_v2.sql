-- depends_on: {{ ref('fact_optimism_transactions_v2') }}
{{
    config(
        materialized="incremental",
        unique_key="tx_hash",
        snowflake_warehouse="optimism_md",
        database="optimism",
        schema="raw",
        alias="ez_transactions_v2",
    )
}}

{{ create_ez_transactions("optimism", "v2") }}
