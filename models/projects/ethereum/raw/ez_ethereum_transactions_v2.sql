-- depends_on: {{ ref('fact_ethereum_transactions') }}
{{
    config(
        materialized="incremental",
        unique_key="tx_hash",
        snowflake_warehouse="ETHEREUM_LG",
        database="ethereum",
        schema="raw",
        alias="ez_transactions_v2",
    )
}}

{{ create_ez_transactions("ethereum", "v2") }}
