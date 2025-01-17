{{
    config(
        materialized="table",
        snowflake_warehouse="ONDO",
    )
}}

{{ rwa_data_by_product_for_issuer("ondo") }}