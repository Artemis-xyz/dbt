-- depends_on: {{ ref('fact_polygon_transactions') }}
{{
    config(
        materialized="incremental",
        unique_key="tx_hash",
        snowflake_warehouse="polygon",
        database="polygon",
        schema="raw",
        alias="ez_transactions",
    )
}}

{{ create_ez_transactions("polygon") }}
