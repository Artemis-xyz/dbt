{{config(materialized="table", snowflake_warehouse='STARGATE')}}
{{
    stargate_fees(
        'avalanche'
        , '0x197d1333dea5fe0d6600e9b396c7f1b1cfcc558a'
    )
}}