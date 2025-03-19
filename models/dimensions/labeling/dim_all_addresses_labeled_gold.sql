-- This table is a full refresh to account for deleted rows inside dim_all_addresses_labeled_silver
{{
    config(
        materialized="table",
        on_schema_change="append_new_columns"
    )
}}

SELECT 
    * 
FROM {{ ref("dim_all_addresses_labeled_silver") }}
WHERE artemis_application_id IS NOT NULL