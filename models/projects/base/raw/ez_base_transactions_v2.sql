-- depends_on: {{ ref('fact_base_transactions') }}
{{
    config(
        materialized="incremental",
        unique_key="tx_hash",
        snowflake_warehouse="base_md",
        database="base",
        schema="raw",
        alias="ez_transactions_v2",
    )
}}

{{ create_ez_transactions("base", "v2") }}
