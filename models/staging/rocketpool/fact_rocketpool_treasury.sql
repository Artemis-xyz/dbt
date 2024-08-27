{{ config(
    materialized="table",
    snowflake_warehouse="ROCKETPOOL"
    )
}}

{{
    get_treasury_balance( "ethereum", ("0x6efD08303F42EDb68F2D6464BCdCA0824e1C813a", "0xb867EA3bBC909954d737019FEf5AB25dFDb38CB9"), '2022-11-01')
}}