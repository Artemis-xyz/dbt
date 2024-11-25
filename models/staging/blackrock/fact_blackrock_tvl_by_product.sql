{{
    config(
        materialized="table",
        snowflake_warehouse="BLACKROCK",
    )
}}

{{ rwa_data_by_product_for_issuer("blackrock") }}