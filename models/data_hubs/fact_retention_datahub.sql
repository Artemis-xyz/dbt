{{ config(materialized="table", snowflake_warehouse="RETENTION") }}

with retention_data as (
    {{
        dbt_utils.union_relations(
            relations=[
                ref("ez_arbitrum_retention"),
                ref("ez_avalanche_retention"),
                ref("ez_base_retention"),
                ref("ez_bsc_retention"),
                ref("ez_ethereum_retention"),
                ref("ez_near_retention"),
                ref("ez_optimism_retention"),
                ref("ez_polygon_retention"),
                ref("ez_tron_retention")
            ],
        )
    }}
)
SELECT
    concat(
        coalesce(cast(chain as string), '_this_is_null_'),
        '|',
        coalesce(cast(cohort_month as string), '_this_is_null_'),
        '|',
        coalesce(cast(month_number as string), '_this_is_null_')
    ) as unique_id,
    chain,
    cohort_month, 
    cohort_size, 
    month_number,
    retention_ratio
FROM retention_data
