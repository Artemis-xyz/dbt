{{ config(materialized="table") }}

select
    coalesce(sigma.id, existing.id) as id,
    coalesce(sigma.category, existing.category) as category,
    coalesce(
        sigma.category_display_name, existing.category_display_name
    ) as category_display_name
from {{ source("MANUAL_STATIC_TABLES", "dim_categories") }} as existing
full join
    (
        select *
        from {{ source("SIGMA", "sigma_new_categories") }}
        where category is not null
    ) as sigma
    on existing.category = sigma.category
