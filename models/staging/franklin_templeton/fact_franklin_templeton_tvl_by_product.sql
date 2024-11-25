{{
    config(
        materialized="table",
        snowflake_warehouse="FRANKLIN_TEMPLETON",
    )
}}

{{ rwa_data_by_product_for_issuer("Franklin Templeton") }}
