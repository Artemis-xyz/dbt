{{config(materialized="table", snowflake_warehouse='BAM_TRENDING_WAREHOUSE_LG')}}
{{
    stargate_asset_map(
        'mantle'
        , '0x41B491285A4f888F9f636cEc8a363AB9770a0AEF'
    )
}}
