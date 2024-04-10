{{ config(materialized="table", snowflake_warehouse="RETENTION") }}

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