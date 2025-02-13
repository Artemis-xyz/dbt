-- depends_on: {{ ref('fact_mantle_transactions') }}
{{
    config(
        materialized="incremental",
        unique_key="tx_hash",
        snowflake_warehouse="bam_transaction_sm",
        database="mantle",
        schema="raw",
        alias="ez_transactions_v2",
    )
}}

{{ create_ez_transactions("mantle", "v2") }}