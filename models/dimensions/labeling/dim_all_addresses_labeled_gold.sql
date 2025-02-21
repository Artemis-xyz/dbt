{{
    config(
        materialized="incremental",
        unique_key=["address", "chain"],
        incremental_strategy="merge",
        on_schema_change="append_new_columns"
    )
}}

SELECT * FROM {{ ref("dim_all_addresses_labeled_silver") }}
{% if is_incremental() %}
    WHERE last_updated > (SELECT MAX(last_updated) FROM {{ this }})
{% endif %}