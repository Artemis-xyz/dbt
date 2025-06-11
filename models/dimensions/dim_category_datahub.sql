{{ config(materialized="table") }}

select 
    concat(coalesce(artemis_category_id, '_this_is_null_'), '|', coalesce(artemis_sub_category_id, '_this_is_null_')) as unique_id,
    artemis_category_id,
    artemis_sub_category_id,
    category_display_name,
    sub_category_display_name,
from {{ source('MANUAL_STATIC_TABLES', 'all_categories_seed') }}