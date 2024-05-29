-- depends_on: {{ ref('fact_starknet_transactions') }}
{{
    config(
        materialized="incremental",
        unique_key="tx_hash",
        snowflake_warehouse="STARKNET_SM",
        database="starknet",
        schema="raw",
        alias="ez_transactions",
    )
}}

{{ create_ez_transactions("starknet") }}
