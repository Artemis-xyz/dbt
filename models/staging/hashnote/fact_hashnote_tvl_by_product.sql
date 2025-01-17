{{
    config(
        materialized="table",
        snowflake_warehouse="HASHNOTE",
    )
}}

{{ rwa_data_by_product_for_issuer("hashnote") }}
