-- depends_on: {{ ref('fact_bsc_transactions_v2') }}
{{
    config(
        materialized="incremental",
        unique_key="tx_hash",
        snowflake_warehouse="BSC",
        database="bsc",
        schema="raw",
        alias="ez_transactions_v2",
    )
}}

{{ create_ez_transactions("bsc", "v2") }}
