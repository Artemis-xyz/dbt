-- depends_on: {{ ref('fact_blast_transactions') }}
{{
    config(
        materialized="incremental",
        unique_key="tx_hash",
        snowflake_warehouse="blast",
        database="blast",
        schema="raw",
        alias="ez_transactions",
    )
}}

{{ create_ez_transactions("blast") }}
