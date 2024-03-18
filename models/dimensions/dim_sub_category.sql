{{ config(materialized="table") }}

select
    coalesce(sigma.id, existing.id) as id,
    coalesce(sigma.sub_category, existing.sub_category) as sub_category,
    coalesce(
        sigma.sub_category_display_name, existing.sub_category_display
    ) as sub_category_display
from {{ source("MANUAL_STATIC_TABLES", "dim_sub_categories") }} as existing
full join
    (
        select *
        from {{ source("SIGMA", "sigma_new_sub_categories") }}
        where sub_category is not null
    ) as sigma
    on existing.sub_category = sigma.sub_category
