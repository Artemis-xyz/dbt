{{config(materialized="table", snowflake_warehouse='STARGATE')}}
{{
    stargate_fees(
        'polygon'
        , '0x6c26c61a97006888ea9E4FA36584c7df57Cd9dA3'
    )
}}