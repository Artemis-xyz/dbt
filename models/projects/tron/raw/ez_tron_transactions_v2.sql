-- depends_on: {{ ref("fact_tron_transactions_v2") }}
{{
    config(
        materialized="incremental",
        unique_key="tx_hash",
        snowflake_warehouse="TRON_LG",
        database="tron",
        schema="raw",
        alias="ez_transactions_v2",
    )
}}

{{ create_ez_transactions("tron", "v2") }}
