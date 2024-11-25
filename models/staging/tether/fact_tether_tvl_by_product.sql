{{
    config(
        materialized="table",
        snowflake_warehouse="TETHER",
    )
}}

{{ rwa_data_by_product_for_issuer("tether") }}
