{{ config(materialized="table", snowflake_warehouse='BRIDGE_MD') }}

with
    flows_by_super_chain as (
        {{
            dbt_utils.union_relations(
                relations=[
                    ref("fact_unichain_bridge_transfers"),
                ]
            )
        }}
    )