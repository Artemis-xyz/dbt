-- depends_on: {{ ref('fact_near_transactions') }}
{{
    config(
        materialized="incremental",
        unique_key="tx_hash",
        snowflake_warehouse="NEAR",
        database="near",
        schema="raw",
        alias="ez_transactions_v2",
    )
}}

{{ create_ez_transactions("near", "v2") }}
