{{ config(materialized="table") }}

select 
    artemis_category_id,
    artemis_sub_category_id,
    initcap(replace(artemis_category_id, '_', ' ')) as category_display_name,
    initcap(replace(artemis_sub_category_id, '_', ' ')) as sub_category_display_name,
from {{ source('MANUAL_STATIC_TABLES', 'all_categories_seed') }}