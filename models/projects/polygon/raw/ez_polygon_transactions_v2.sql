-- depends_on: {{ ref('fact_polygon_transactions') }}
{{
    config(
        materialized="incremental",
        unique_key="tx_hash",
        snowflake_warehouse="polygon_xlg",
        database="polygon",
        schema="raw",
        alias="ez_transactions_v2",
    )
}}

{{ create_ez_transactions("polygon", "v2") }}
