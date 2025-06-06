{{ config(materialized="table", snowflake_warehouse="RETENTION") }}

with retention_data as (
    {{
        dbt_utils.union_relations(
            relations=[
                ref("ez_solana_app_retention_metrics"),
            ],
        )
    }}
)
SELECT
    concat(
        coalesce(cast(chain as string), '_this_is_null_'),
        '|',
        coalesce(cast(app as string), '_this_is_null_'),
        '|',
        coalesce(cast(cohort_month as string), '_this_is_null_'),
        '|',
        coalesce(cast(month_number as string), '_this_is_null_')
    ) as unique_id,
    chain,
    app,
    cohort_month, 
    cohort_size, 
    month_number,
    retained_user_count,
    retention_ratio
FROM retention_data
