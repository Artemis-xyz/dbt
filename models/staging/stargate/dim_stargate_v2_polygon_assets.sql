{{config(materialized="table", snowflake_warehouse='BAM_TRENDING_WAREHOUSE_LG')}}
{{
    stargate_asset_map(
        'polygon'
        , '0x6CE9bf8CDaB780416AD1fd87b318A077D2f50EaC'
    )
}}
