{{
    config(
        materialized="table"
    )
}}

with temp as (
select distinct category, sub_category from dim_apps_gold where category is not null)
select category, ARRAY_AGG(sub_category) as sub_category from temp group by category;