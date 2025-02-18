{{
    config(
        materialized='table',
        snowflake_warehouse='LIQUITY',
        database='LIQUITY',
        schema='raw',
        alias='fact_liquity_outstanding_supply'
    )
}}

with agg_outstanding_supply as (
    {{
        dbt_utils.union_relations(
            relations=[
                ref('fact_liquity_v1_outstanding_supply_ethereum'),
                ref('fact_liquity_v1_outstanding_supply_polygon'),
                ref('fact_liquity_v2_outstanding_supply')
            ]
        )
    }}
)

select * from agg_outstanding_supply