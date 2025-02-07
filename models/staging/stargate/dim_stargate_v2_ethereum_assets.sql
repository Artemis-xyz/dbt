{{config(materialized="table", snowflake_warehouse='BAM_TRENDING_WAREHOUSE_LG')}}

{{
    stargate_asset_map(
        'ethereum'
        , '0x6d6620eFa72948C5f68A3C8646d58C00d3f4A980'
        , '0x77b2043768d28e9c9ab44e1abfc95944bce57931'
        , '0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2'
    )
}}
