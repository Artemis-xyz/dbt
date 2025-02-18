{{
    config(
        materialized='table',
        snowflake_warehouse='LIQUITY',
        database='LIQUITY',
        schema='raw',
        alias='fact_liquity_tvl'
    )
}}

with agg_tvl as (
    {{
        dbt_utils.union_relations(
            relations=[
                ref('fact_liquity_v1_tvl'),
                ref('fact_liquity_v2_tvl')
            ]
        )
    }}
)

select * from agg_tvl