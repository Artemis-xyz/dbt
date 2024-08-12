{{ config(materialized="table") }}

with daily_data as (
        {{
            dbt_utils.union_relations(
                relations=[
                    ref("fact_optimism_category_inflows"),
                ]
            )
        }}
   )
SELECT
    chain,
    date,
    from_app,
    from_friendly_name,
    application_amount_usd,
    to_category,
    category_amount_usd,
    category
FROM daily_data
