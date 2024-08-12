{{ config(materialized="table") }}

with daily_data as (
        {{
            dbt_utils.union_relations(
                relations=[
                    ref("fact_optimism_category_outflows"),
                ]
            )
        }}
   )
SELECT
    chain,
    date,
    category,
    to_category,
    category_amount_usd,
    to_app,
    to_friendly_name,
    application_amount_usd,
    date::string || '-' || chain || '-' || category || '-' || to_category || '-' || to_app as unique_id
FROM daily_data
