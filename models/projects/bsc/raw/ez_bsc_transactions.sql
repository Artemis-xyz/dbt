-- depends_on: {{ ref('fact_bsc_transactions') }}
{{
    config(
        materialized="incremental",
        unique_key="tx_hash",
        snowflake_warehouse="BSC_MD",
        database="bsc",
        schema="raw",
        alias="ez_transactions",
    )
}}

{{ create_ez_transactions("bsc") }}
