{{
    config(
        materialized="table",
        on_schema_change="append_new_columns"
    )
}}

SELECT * FROM {{ ref("dim_all_addresses_labeled_silver") }}