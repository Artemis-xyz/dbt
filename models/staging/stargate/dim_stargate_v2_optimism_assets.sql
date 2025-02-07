{{config(materialized="table", snowflake_warehouse='BAM_TRENDING_WAREHOUSE_LG')}}
{{
    stargate_asset_map(
        'optimism'
        , '0xF1fCb4CBd57B67d683972A59B6a7b1e2E8Bf27E6'
        , '0xe8CDF27AcD73a434D661C84887215F7598e7d0d3'
        , '0x4200000000000000000000000000000000000006'
    )
}}
