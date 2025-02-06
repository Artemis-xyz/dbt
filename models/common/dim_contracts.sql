{{
    config(
        snowflake_warehouse="COMMON",
        database="common",
        schema="core",
        materialized='view'
    )
}}


select
    address,
    name,
    friendly_name,
    artemis_application_id AS app,
    artemis_category_id AS category,
    artemis_sub_category_id AS sub_category,
    chain
from {{ref("dim_all_addresses_labeled_gold")}}