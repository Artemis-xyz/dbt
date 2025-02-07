{{config(materialized="table", snowflake_warehouse='BAM_TRENDING_WAREHOUSE_LG')}}
{{
    stargate_asset_map(
        'avalanche'
        , '0x17E450Be3Ba9557F2378E20d64AD417E59Ef9A34'
    )
}}
