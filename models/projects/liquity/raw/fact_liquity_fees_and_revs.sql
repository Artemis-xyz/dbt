{{
    config(
        materialized='table',
        snowflake_warehouse='LIQUITY',
        database='LIQUITY',
        schema='raw',
        alias='fact_liquity_fees_and_revs'
    )
}}
with agg_fees_and_revs as (
    {{
        dbt_utils.union_relations(
            relations=[
                ref('fact_liquity_v1_fees_and_revs'),
                ref('fact_liquity_v2_fees_and_revs')
            ]
        )
    }}
)
select * from agg_fees_and_revs