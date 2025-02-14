{{config(materialized="table", snowflake_warehouse='STARGATE')}}
{{
    stargate_fees(
        'arbitrum'
        , '0x975bcD720be66659e3EB3C0e4F1866a3020E493A'
        , '0x912CE59144191C1204E64559FE8253a0e49E6548'
    )
}}