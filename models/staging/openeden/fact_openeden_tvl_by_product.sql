{{
    config(
        materialized="table",
        snowflake_warehouse="OPENEDEN",
    )
}}

{{ rwa_data_by_product_for_issuer("OpenEden") }}
