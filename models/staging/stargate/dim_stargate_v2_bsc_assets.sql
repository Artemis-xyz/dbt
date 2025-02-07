{{config(materialized="table", snowflake_warehouse='BAM_TRENDING_WAREHOUSE_LG')}}
{{
    stargate_asset_map(
        'bsc'
        , '0x6E3d884C96d640526F273C61dfcF08915eBd7e2B'
    )
}}
